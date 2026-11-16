package example.scalaspark

import java.net.InetSocketAddress
import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.sql.Date
import java.time.LocalDate
import java.time.format.DateTimeFormatter

import com.fasterxml.jackson.databind.ObjectMapper
import com.github.mrpowers.spark.daria.sql.DariaValidator.{
  validateAbsenceOfColumns,
  validatePresenceOfColumns,
  validateSchema
}
import com.github.mrpowers.spark.daria.sql.functions.{
  arrayExNull,
  bucketFinder,
  removeAllWhitespace,
  singleSpace,
  truncate,
  yeardiff
}
import com.github.mrpowers.spark.daria.sql.transformations.{
  multiRegexpReplace,
  snakeCaseColumns,
  snakifyColumns,
  sortColumns
}
import com.github.mrpowers.spark.daria.sql.{ColumnExt, CustomTransform, DataFrameExt, EtlDefinition}
import com.sun.net.httpserver.{HttpExchange, HttpServer}
import org.apache.spark.sql.functions.{
  coalesce,
  col,
  count,
  countDistinct,
  expr,
  lit,
  lower,
  regexp_replace,
  round,
  size,
  sum,
  trim,
  try_to_date,
  upper,
  when
}
import org.apache.spark.sql.types.{
  ArrayType,
  BooleanType,
  DateType,
  DoubleType,
  IntegerType,
  LongType,
  StringType,
  StructField,
  StructType
}
import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}

import scala.collection.mutable
import scala.jdk.CollectionConverters._

final case class IngestResult(bronzePath: String, rows: Long, uniqueIds: Long, pages: Int, sourceUrl: String, runRoot: String)
final case class TransformResult(silverPath: String, bronzeRows: Long, silverRows: Long, checksum: Long, quality: Map[String, Any])
final case class GoldResult(
    revenuePath: String,
    segmentsPath: String,
    vipPath: String,
    revenueRows: Long,
    segmentRows: Long,
    kpis: Map[String, Any]
)

object Pipeline {
  val AsOfDate: Date = Date.valueOf("2026-11-16")

  val BronzeScalarColumns: Seq[String] = Seq(
    "OrderID",
    "Customer Name",
    "emailAddress",
    "AMOUNT_USD",
    "Order Date",
    "status",
    "Country Code",
    "Qty",
    "notes",
    "isVIP"
  )
  val BronzeColumns: Seq[String] = BronzeScalarColumns ++ Seq("Address", "LineItems")

  val AddressSchema: StructType = StructType(
    Seq(
      StructField("City", StringType, nullable = true),
      StructField("Country", StringType, nullable = true),
      StructField("PostalCode", StringType, nullable = true)
    )
  )
  val LineItemSchema: StructType = StructType(
    Seq(StructField("sku", StringType, nullable = true), StructField("qty", StringType, nullable = true))
  )
  val BronzeSchema: StructType = StructType(
    BronzeScalarColumns.map(c => StructField(c, StringType, nullable = true)) ++ Seq(
      StructField("Address", AddressSchema, nullable = true),
      StructField("LineItems", ArrayType(LineItemSchema, containsNull = true), nullable = true)
    )
  )

  val SilverColumns: Seq[String] = Seq(
    "order_id",
    "customer_name",
    "email_address",
    "amount_usd",
    "order_date",
    "status",
    "country_code",
    "qty",
    "notes",
    "is_vip",
    "address_city",
    "address_country",
    "address_postal_code",
    "address_parts",
    "line_items",
    "line_item_count",
    "years_since_order",
    "email_is_blank"
  )

  val SilverSchema: StructType = StructType(
    Seq(
      StructField("order_id", LongType, nullable = true),
      StructField("customer_name", StringType, nullable = true),
      StructField("email_address", StringType, nullable = true),
      StructField("amount_usd", DoubleType, nullable = true),
      StructField("order_date", DateType, nullable = true),
      StructField("status", StringType, nullable = true),
      StructField("country_code", StringType, nullable = true),
      StructField("qty", IntegerType, nullable = true),
      StructField("notes", StringType, nullable = true),
      StructField("is_vip", BooleanType, nullable = false),
      StructField("address_city", StringType, nullable = true),
      StructField("address_country", StringType, nullable = true),
      StructField("address_postal_code", StringType, nullable = true),
      StructField("address_parts", ArrayType(StringType, containsNull = false), nullable = false),
      StructField("line_items", ArrayType(LineItemSchema, containsNull = true), nullable = true),
      StructField("line_item_count", IntegerType, nullable = true),
      StructField("years_since_order", DoubleType, nullable = true),
      StructField("email_is_blank", BooleanType, nullable = true)
    )
  )

  val DefaultUniqueRows: Long = sys.env.get("PIPELINE_ROWS").map(_.toLong).getOrElse(2000L)
  val DefaultPageSize: Int = sys.env.get("PIPELINE_PAGE_SIZE").map(_.toInt).getOrElse(200)

  def rootDir(runId: String): String = {
    val base = sys.env.getOrElse(
      "PIPELINE_ROOT",
      if (Files.isDirectory(Paths.get("/usr/local/airflow/include")))
        "/usr/local/airflow/include/pipeline"
      else "/tmp/pipeline"
    )
    val safe = runId.replaceAll("[^A-Za-z0-9_.-]", "_")
    val path = Paths.get(base, safe).toString
    Files.createDirectories(Paths.get(path))
    path
  }

  def duplicateCount(uniqueRows: Long): Long = uniqueRows / 20
  def bronzeCount(uniqueRows: Long): Long = uniqueRows + duplicateCount(uniqueRows)

  def jsonRecord(i: Long): String = {
    val scalars = BronzeScalarColumns
      .map { field =>
        val v = rawValue(i, field)
        val rendered = if (v == null) "null" else "\"" + escape(v) + "\""
        s""""$field": $rendered"""
      }
      .mkString(", ")
    val city = if (i % 31 == 0) null else s"  ${Seq("Buenos Aires", "Cordoba", "Rosario", "Mendoza", "Salta")((i % 5).toInt)}  "
    val addrCountry = if (i % 29 == 0) null else rawValue(i, "Country Code")
    val postal = if (i % 37 == 0) null else s" C${1000 + (i % 90)} "
    val cityJs = if (city == null) "null" else "\"" + escape(city) + "\""
    val countryJs = if (addrCountry == null) "null" else "\"" + escape(addrCountry) + "\""
    val postalJs = if (postal == null) "null" else "\"" + escape(postal) + "\""
    val skuA = f"SKU-${(i % 40) + 1}%02d"
    val skuB = f"SKU-${((i + 3) % 40) + 1}%02d"
    val items =
      if (i % 41 == 0) "null"
      else s"""[{"sku": "$skuA", "qty": "${(i % 3) + 1}"}, null, {"sku": "$skuB", "qty": "1"}]"""
    s"""{$scalars, "Address": {"City": $cityJs, "Country": $countryJs, "PostalCode": $postalJs}, "LineItems": $items}"""
  }

  def bronzeFrame(spark: SparkSession, uniqueRows: Long = DefaultUniqueRows): DataFrame = {
    val ids = (0L until uniqueRows) ++ (0L until duplicateCount(uniqueRows))
    val json = ids.map(jsonRecord).toSeq
    spark.read.schema(BronzeSchema).json(spark.createDataset(json)(Encoders.STRING))
  }

  def ingest(
      spark: SparkSession,
      bronzePath: String,
      uniqueRows: Long = DefaultUniqueRows,
      pageSize: Int = DefaultPageSize
  ): IngestResult = {
    val server = FakeOrdersApi.start(uniqueRows, pageSize)
    try {
      val base = s"http://127.0.0.1:${server.getAddress.getPort}"
      val pages = FakeOrdersApi.fetchPages(base)
      val df = spark.read.schema(BronzeSchema).json(spark.createDataset(pages.flatMap(_._2))(Encoders.STRING))
      df.write.mode("overwrite").parquet(bronzePath)
      val rows = df.count()
      val unique = df.select("OrderID").na.drop().distinct().count()
      val runRoot = Paths.get(bronzePath).getParent.toString
      IngestResult(bronzePath, rows, unique, pages.size, s"$base/orders", runRoot)
    } finally server.stop(0)
  }

  def withDaria(df: DataFrame): DataFrame = {
    import ColumnExt._
    import DataFrameExt._
    validatePresenceOfColumns(df, BronzeColumns)
    val flattened = df.transform((d: DataFrame) => d.flattenSchema("_"))
    val renamed = flattened.transform(snakifyColumns()).transform(snakeCaseColumns())
    validateAbsenceOfColumns(
      renamed,
      Seq(
        "OrderID",
        "Customer Name",
        "emailAddress",
        "AMOUNT_USD",
        "Order Date",
        "Country Code",
        "Qty",
        "isVIP",
        "Address",
        "LineItems",
        "Address_City",
        "Address_Country",
        "Address_PostalCode"
      )
    )
    val cleaned = renamed.composeTransforms(
      (d: DataFrame) =>
        d.transform(multiRegexpReplace(List(col("customer_name"), col("notes")), "[^A-Za-z0-9 @._-]", " ")),
      (d: DataFrame) => d.withColumn("customer_name", singleSpace(col("customer_name"))),
      (d: DataFrame) => d.withColumn("email_address", lower(trim(col("email_address")))),
      (d: DataFrame) =>
        d.withColumn("amount_usd", regexp_replace(trim(col("amount_usd")), ",", "").cast(DoubleType)),
      (d: DataFrame) =>
        d.withColumn(
          "order_date",
          coalesce(
            try_to_date(trim(col("order_date")), "yyyy-MM-dd"),
            try_to_date(trim(col("order_date")), "dd/MM/yyyy")
          )
        ),
      (d: DataFrame) => d.withColumn("status", lower(trim(col("status")))),
      (d: DataFrame) => d.withColumn("country_code", upper(trim(col("country_code")))),
      (d: DataFrame) => d.withColumn("qty", trim(col("qty")).cast(IntegerType)),
      (d: DataFrame) => d.withColumn("notes", truncate(singleSpace(col("notes")), 40)),
      (d: DataFrame) => d.withColumn("is_vip", vipFlag(col("is_vip"))),
      (d: DataFrame) => d.withColumn("order_id", trim(col("order_id")).cast(LongType)),
      (d: DataFrame) => d.withColumn("address_city", singleSpace(col("address_city"))),
      (d: DataFrame) => d.withColumn("address_country", upper(trim(col("address_country")))),
      (d: DataFrame) => d.withColumn("address_postal_code", removeAllWhitespace(col("address_postal_code"))),
      (d: DataFrame) =>
        d.withColumn(
          "address_parts",
          arrayExNull(col("address_city"), col("address_country"), col("address_postal_code"))
        ),
      (d: DataFrame) =>
        d.withColumn("line_items", expr("filter(line_items, x -> x is not null)")),
      (d: DataFrame) => d.withColumn("line_item_count", size(col("line_items"))),
      (d: DataFrame) => d.where(col("order_id").isNotNull),
      (d: DataFrame) => d.dropDuplicates("order_id")
    )
    val recency = CustomTransform(
      (d: DataFrame) => d.withColumn("years_since_order", yeardiff(lit(AsOfDate), col("order_date"))),
      Seq("order_date"),
      Seq("years_since_order"),
      Seq.empty,
      skipWhenPossible = false
    )
    val blankEmail = CustomTransform(
      (d: DataFrame) => d.withColumn("email_is_blank", col("email_address").isNullOrBlank),
      Seq("email_address"),
      Seq("email_is_blank"),
      Seq.empty,
      skipWhenPossible = false
    )
    val decorated = cleaned.trans(recency).trans(blankEmail)
    val selected = decorated
      .select(SilverColumns.map(col): _*)
      .reorderColumns(SilverColumns)
    validateSchema(selected, SilverSchema)
    selected
  }

  def withPlain(df: DataFrame): DataFrame = {
    val missing = BronzeColumns.diff(df.columns.toSeq)
    if (missing.nonEmpty) {
      throw new IllegalArgumentException(
        s"The [${missing.mkString(", ")}] columns are not included in the DataFrame with the following columns [${df.columns.mkString(", ")}]"
      )
    }
    import DataFrameExt._
    val flattened = df.flattenSchema("_")
    val renamed = flattened.columns.foldLeft(flattened) { (memo, name) =>
      memo.withColumnRenamed(name, toSnakeCase(name))
    }
    renamed
      .withColumn("customer_name", regexp_replace(trim(col("customer_name")), " +", " "))
      .withColumn("email_address", lower(trim(col("email_address"))))
      .withColumn("amount_usd", regexp_replace(trim(col("amount_usd")), ",", "").cast(DoubleType))
      .withColumn(
        "order_date",
        coalesce(
          try_to_date(trim(col("order_date")), "yyyy-MM-dd"),
          try_to_date(trim(col("order_date")), "dd/MM/yyyy")
        )
      )
      .withColumn("status", lower(trim(col("status"))))
      .withColumn("country_code", upper(trim(col("country_code"))))
      .withColumn("qty", trim(col("qty")).cast(IntegerType))
      .withColumn("notes", regexp_replace(trim(col("notes")), " +", " "))
      .withColumn("is_vip", vipFlag(col("is_vip")))
      .withColumn("order_id", trim(col("order_id")).cast(LongType))
      .withColumn("address_city", regexp_replace(trim(col("address_city")), " +", " "))
      .withColumn("address_country", upper(trim(col("address_country"))))
      .withColumn("address_postal_code", regexp_replace(col("address_postal_code"), "\\s+", ""))
      .withColumn(
        "address_parts",
        arrayExNull(col("address_city"), col("address_country"), col("address_postal_code"))
      )
      .withColumn("line_items", expr("filter(line_items, x -> x is not null)"))
      .withColumn("line_item_count", size(col("line_items")))
      .withColumn("years_since_order", yeardiff(lit(AsOfDate), col("order_date")))
      .withColumn("email_is_blank", col("email_address").isNull || trim(col("email_address")) === "")
      .where(col("order_id").isNotNull)
      .dropDuplicates("order_id")
      .select(SilverColumns.map(col): _*)
  }

  def toSnakeCase(name: String): String =
    name
      .replaceAll("([A-Z]+)([A-Z][a-z])", "$1_$2")
      .replaceAll("([a-z\\d])([A-Z])", "$1_$2")
      .replaceAll("\\s+", "_")
      .toLowerCase

  def checksum(df: DataFrame): Long = {
    val row = df
      .agg(
        sum(col("order_id")).as("ids"),
        sum(coalesce(col("qty").cast(LongType), lit(0L))).as("qty"),
        sum(coalesce(round(col("amount_usd") * 100, 0).cast(LongType), lit(0L))).as("cents")
      )
      .first()
    row.getLong(0) + row.getLong(1) + row.getLong(2)
  }

  def qualityReport(bronze: DataFrame, silver: DataFrame): Map[String, Any] = {
    import DataFrameExt._
    val killed = bronze.transform((d: DataFrame) => d.killDuplicates()).count()
    val blankEmails = silver.filter(col("email_is_blank")).count()
    Map(
      "bronze_rows" -> bronze.count(),
      "silver_rows" -> silver.count(),
      "exact_duplicate_groups_killDuplicates_keeps" -> killed,
      "blank_emails" -> blankEmails,
      "null_amounts" -> silver.filter(col("amount_usd").isNull).count(),
      "vip_rows" -> silver.filter(col("is_vip")).count(),
      "validations" -> Seq(
        Map("name" -> "validatePresenceOfColumns", "status" -> "passed"),
        Map("name" -> "validateAbsenceOfColumns", "status" -> "passed"),
        Map("name" -> "validateSchema", "status" -> "passed")
      )
    )
  }

  def transform(
      spark: SparkSession,
      bronzePath: String,
      silverPath: String,
      mode: String = "daria"
  ): TransformResult = {
    val bronze = spark.read.schema(BronzeSchema).parquet(bronzePath)
    val bronzeRows = bronze.count()
    val silver = mode match {
      case "plain" => withPlain(bronze)
      case _       => withDaria(bronze)
    }
    silver.write.mode("overwrite").parquet(silverPath)
    val written = spark.read.parquet(silverPath)
    val quality = qualityReport(bronze, written)
    TransformResult(silverPath, bronzeRows, written.count(), checksum(written), quality)
  }

  def toGold(spark: SparkSession, silverPath: String, goldRoot: String): GoldResult = {
    import DataFrameExt._
    val silver = spark.read.parquet(silverPath)
    val revenuePath = s"$goldRoot/revenue_by_country_month"
    val segmentsPath = s"$goldRoot/customer_segments"
    val vipPath = s"$goldRoot/vip_metrics"

    val revenueEtl = EtlDefinition(
      silver,
      (d: DataFrame) =>
        d.withColumn("month", expr("date_format(order_date, 'yyyy-MM')"))
          .groupBy("country_code", "month")
          .agg(
            count(lit(1)).as("order_count"),
            sum(coalesce(col("amount_usd"), lit(0.0))).as("revenue"),
            sum(when(col("is_vip"), coalesce(col("amount_usd"), lit(0.0))).otherwise(0.0)).as("vip_revenue"),
            sum(when(col("is_vip"), 1).otherwise(0)).as("vip_orders")
          )
          .orderBy(col("month"), col("country_code")),
      (d: DataFrame) => { d.write.mode("overwrite").parquet(revenuePath); () },
      mutable.Map[String, Any]("layer" -> "gold", "table" -> "revenue_by_country_month")
    )
    revenueEtl.process()

    // spark-daria 1.2.4 passes these bounds to lit(); Spark 4.3 rejects Scala Option
    // literals, so use null / boxed doubles, which is what lit() + isNull expects.
    val buckets: Array[(Any, Any)] = Array(
      (null, java.lang.Double.valueOf(50.0)),
      (java.lang.Double.valueOf(50.0), java.lang.Double.valueOf(200.0)),
      (java.lang.Double.valueOf(200.0), java.lang.Double.valueOf(1000.0)),
      (java.lang.Double.valueOf(1000.0), null)
    )
    val segmentsEtl = EtlDefinition(
      silver,
      (d: DataFrame) => {
        val byCustomer = d
          .groupBy("email_address", "customer_name")
          .agg(
            sum(coalesce(col("amount_usd"), lit(0.0))).as("total_spend"),
            count(lit(1)).as("order_count"),
            sum(when(col("is_vip"), 1).otherwise(0)).as("vip_orders")
          )
        byCustomer.withColumn(
          "segment",
          bucketFinder(col("total_spend"), buckets, inclusiveBoundries = true, lowestBoundLte = true, highestBoundGte = true)
        )
      },
      (d: DataFrame) => { d.write.mode("overwrite").parquet(segmentsPath); () },
      mutable.Map[String, Any]("layer" -> "gold", "table" -> "customer_segments")
    )
    segmentsEtl.process()

    val revenue = spark.read.parquet(revenuePath)
    val segments = spark.read.parquet(segmentsPath)
    val vip = silver
      .agg(
        count(lit(1)).as("orders"),
        countDistinct(col("email_address")).as("customers"),
        sum(coalesce(col("amount_usd"), lit(0.0))).as("revenue"),
        sum(when(col("is_vip"), 1).otherwise(0)).as("vip_orders"),
        countDistinct(when(col("is_vip"), col("email_address"))).as("vip_customers"),
        sum(when(col("is_vip"), coalesce(col("amount_usd"), lit(0.0))).otherwise(0.0)).as("vip_revenue")
      )
      .withColumn("vip_revenue_share", col("vip_revenue") / col("revenue"))
    vip.write.mode("overwrite").parquet(vipPath)

    val kpiRow = spark.read.parquet(vipPath).first()
    val kpis = Map[String, Any](
      "orders" -> kpiRow.getAs[Number]("orders").longValue(),
      "customers" -> kpiRow.getAs[Number]("customers").longValue(),
      "revenue" -> kpiRow.getAs[Number]("revenue").doubleValue(),
      "vip_orders" -> kpiRow.getAs[Number]("vip_orders").longValue(),
      "vip_customers" -> kpiRow.getAs[Number]("vip_customers").longValue(),
      "vip_revenue" -> kpiRow.getAs[Number]("vip_revenue").doubleValue(),
      "vip_revenue_share" -> kpiRow.getAs[Number]("vip_revenue_share").doubleValue()
    )
    GoldResult(revenuePath, segmentsPath, vipPath, revenue.count(), segments.count(), kpis)
  }

  def schemaFields(df: DataFrame): Seq[Map[String, String]] =
    df.schema.fields.toSeq.map(f => Map("name" -> f.name, "type" -> f.dataType.simpleString, "nullable" -> String.valueOf(f.nullable)))

  def writeManifest(runRoot: String, ingest: IngestResult, silver: TransformResult, gold: GoldResult, spark: SparkSession): Unit = {
    val bronze = spark.read.schema(BronzeSchema).parquet(ingest.bronzePath)
    val sil = spark.read.parquet(silver.silverPath)
    val doc = Map[String, Any](
      "run_root" -> runRoot,
      "source_url" -> ingest.sourceUrl,
      "pages" -> ingest.pages,
      "layers" -> Map(
        "bronze" -> Map("path" -> ingest.bronzePath, "rows" -> ingest.rows, "unique_ids" -> ingest.uniqueIds, "schema" -> schemaFields(bronze)),
        "silver" -> Map("path" -> silver.silverPath, "rows" -> silver.silverRows, "checksum" -> silver.checksum, "schema" -> schemaFields(sil)),
        "gold" -> Map(
          "revenue_by_country_month" -> Map("path" -> gold.revenuePath, "rows" -> gold.revenueRows),
          "customer_segments" -> Map("path" -> gold.segmentsPath, "rows" -> gold.segmentRows),
          "vip_metrics" -> Map("path" -> gold.vipPath, "rows" -> 1)
        )
      ),
      "quality" -> silver.quality,
      "kpis" -> gold.kpis,
      "lineage_dir" -> s"$runRoot/lineage"
    )
    Lineage.writeJson(Paths.get(runRoot, "run.json"), doc)
    Lineage.writeSummary(s"$runRoot/lineage", Map("run_root" -> runRoot))
  }

  def vipFlag(c: org.apache.spark.sql.Column): org.apache.spark.sql.Column = {
    val token = lower(trim(c))
    when(token.isInCollection(Seq("y", "yes", "true", "1", "t")), lit(true)).otherwise(lit(false))
  }

  def rawValue(i: Long, field: String): String = {
    val date = LocalDate.of(2024, 1, 1).plusDays(i % 365)
    field match {
      case "OrderID" => (10000L + i).toString
      case "Customer Name" =>
        if (i % 17 == 0) null else s"  User   ${i % 500}  "
      case "emailAddress" =>
        if (i % 19 == 0) "  " else s"  USER${i % 500}@Example.COM  "
      case "AMOUNT_USD" =>
        if (i % 11 == 0) null
        else {
          val cents = ((i * 37) % 500000) + 50 + (if (i % 7 == 0) 100000 else 0)
          val raw = f"${cents / 100.0}%.2f"
          if (cents >= 100000) s"  ${raw.replaceFirst("(\\d+)(\\d{3}\\.)", "$1,$2")} " else s" $raw "
        }
      case "Order Date" =>
        (i % 3) match {
          case 0 => date.format(DateTimeFormatter.ISO_LOCAL_DATE)
          case 1 => date.format(DateTimeFormatter.ofPattern("dd/MM/yyyy"))
          case _ => " " + date.format(DateTimeFormatter.ISO_LOCAL_DATE) + " "
        }
      case "status" =>
        if (i % 13 == 0) null else Seq("paid", " PAID ", "pending", "shipped", " CANCELLED ")((i % 5).toInt)
      case "Country Code" =>
        if (i % 29 == 0) null else Seq("us", " AR ", "de", "br", "  fr")((i % 5).toInt)
      case "Qty" =>
        if (i % 23 == 0) null else s" ${(i % 8).toInt + 1} "
      case "notes" =>
        if (i % 7 == 0) null else "  packed   and   ready  "
      case "isVIP" =>
        Seq("true", "Y", "yes", "1", "false", "N", "0", "", null)((i % 9).toInt)
      case _ => null
    }
  }

  private def escape(s: String): String =
    s.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n")
}

object FakeOrdersApi {
  private val mapper = new ObjectMapper()

  def start(uniqueRows: Long, pageSize: Int): HttpServer = {
    val ids = ((0L until uniqueRows) ++ (0L until Pipeline.duplicateCount(uniqueRows))).toVector
    val totalPages = math.max(1, ((ids.size + pageSize - 1) / pageSize).toInt)
    val server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)
    server.createContext(
      "/orders",
      (ex: HttpExchange) => {
        if (ex.getRequestMethod != "GET") {
          ex.sendResponseHeaders(405, -1)
          ex.close()
        } else {
          val page = queryParam(ex.getRequestURI, "page").getOrElse(1)
          val start = (page - 1) * pageSize
          val slice = ids.slice(start, start + pageSize)
          val items = slice.map(Pipeline.jsonRecord).mkString("[", ", ", "]")
          val body =
            s"""{"page": $page, "page_size": $pageSize, "total_pages": $totalPages, "items": $items}"""
          val bytes = body.getBytes(StandardCharsets.UTF_8)
          ex.getResponseHeaders.add("Content-Type", "application/json")
          ex.sendResponseHeaders(200, bytes.length)
          val out = ex.getResponseBody
          out.write(bytes)
          out.close()
        }
      }
    )
    server.setExecutor(null)
    server.start()
    server
  }

  def fetchPages(baseUrl: String): Seq[(Int, Seq[String])] = {
    val client = HttpClient.newHttpClient()
    val first = fetchPage(client, baseUrl, 1)
    val total = first.get("total_pages").asInt
    (1 to total).map { page =>
      val node = if (page == 1) first else fetchPage(client, baseUrl, page)
      val items = node.get("items")
      val rows = (0 until items.size()).map(i => items.get(i).toString)
      (page, rows)
    }
  }

  private def fetchPage(client: HttpClient, baseUrl: String, page: Int) = {
    val req = HttpRequest.newBuilder(URI.create(s"$baseUrl/orders?page=$page")).GET().build()
    val res = client.send(req, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8))
    if (res.statusCode() != 200) {
      throw new IllegalStateException(s"GET /orders?page=$page returned ${res.statusCode()}")
    }
    mapper.readTree(res.body())
  }

  private def queryParam(uri: URI, name: String): Option[Int] =
    Option(uri.getQuery)
      .getOrElse("")
      .split("&")
      .collectFirst {
        case p if p.startsWith(s"$name=") => p.drop(name.length + 1).toInt
      }
}
