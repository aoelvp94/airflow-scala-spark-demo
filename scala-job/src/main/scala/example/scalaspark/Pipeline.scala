package example.scalaspark

import java.net.InetSocketAddress
import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.time.LocalDate
import java.time.format.DateTimeFormatter

import com.fasterxml.jackson.databind.ObjectMapper
import com.github.mrpowers.spark.daria.sql.DariaValidator.{validatePresenceOfColumns, validateSchema}
import com.github.mrpowers.spark.daria.sql.functions.singleSpace
import com.github.mrpowers.spark.daria.sql.transformations.{snakeCaseColumns, snakifyColumns}
import com.github.mrpowers.spark.daria.sql.DataFrameExt
import com.sun.net.httpserver.{HttpExchange, HttpServer}
import org.apache.spark.sql.functions.{coalesce, col, lit, lower, regexp_replace, round, sum, trim, try_to_date, upper, when}
import org.apache.spark.sql.types.{BooleanType, DateType, DoubleType, IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}

final case class IngestResult(bronzePath: String, rows: Long, uniqueIds: Long, pages: Int, sourceUrl: String)
final case class TransformResult(silverPath: String, bronzeRows: Long, silverRows: Long, checksum: Long)

object Pipeline {
  val BronzeColumns: Seq[String] = Seq(
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

  val BronzeSchema: StructType =
    StructType(BronzeColumns.map(c => StructField(c, StringType, nullable = true)))

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
    "is_vip"
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
      StructField("is_vip", BooleanType, nullable = false)
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

  def jsonRecord(i: Long): String =
    BronzeColumns
      .map { field =>
        val v = rawValue(i, field)
        val rendered = if (v == null) "null" else "\"" + escape(v) + "\""
        s""""$field": $rendered"""
      }
      .mkString("{", ", ", "}")

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
      IngestResult(bronzePath, rows, unique, pages.size, s"$base/orders")
    } finally server.stop(0)
  }

  def withDaria(df: DataFrame): DataFrame = {
    import DataFrameExt._
    validatePresenceOfColumns(df, BronzeColumns)
    val renamed = df.transform(snakifyColumns()).transform(snakeCaseColumns())
    val cleaned = renamed.composeTransforms(
      (d: DataFrame) => d.withColumn("customer_name", singleSpace(col("customer_name"))),
      (d: DataFrame) => d.withColumn("email_address", lower(trim(col("email_address")))),
      (d: DataFrame) =>
        d.withColumn(
          "amount_usd",
          regexp_replace(trim(col("amount_usd")), ",", "").cast(DoubleType)
        ),
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
      (d: DataFrame) => d.withColumn("notes", singleSpace(col("notes"))),
      (d: DataFrame) => d.withColumn("is_vip", vipFlag(col("is_vip"))),
      (d: DataFrame) => d.withColumn("order_id", trim(col("order_id")).cast(LongType)),
      (d: DataFrame) => d.where(col("order_id").isNotNull),
      (d: DataFrame) => d.dropDuplicates("order_id"),
      (d: DataFrame) => d.select(SilverColumns.map(col): _*)
    )
    validateSchema(cleaned, SilverSchema)
    cleaned
  }

  def withPlain(df: DataFrame): DataFrame = {
    val missing = BronzeColumns.diff(df.columns.toSeq)
    if (missing.nonEmpty) {
      throw new IllegalArgumentException(
        s"The [${missing.mkString(", ")}] columns are not included in the DataFrame with the following columns [${df.columns.mkString(", ")}]"
      )
    }
    val renamed = df.columns.foldLeft(df) { (memo, name) =>
      memo.withColumnRenamed(name, toSnakeCase(name))
    }
    val cleaned = renamed
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
      .where(col("order_id").isNotNull)
      .dropDuplicates("order_id")
      .select(SilverColumns.map(col): _*)
    val missingFields = SilverSchema.filterNot(f => cleaned.schema.fields.exists(_ == f))
    if (missingFields.nonEmpty) {
      throw new IllegalArgumentException(
        s"The [${missingFields.mkString(", ")}] StructFields are not included in the DataFrame with the following StructFields [${cleaned.schema}]"
      )
    }
    cleaned
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
    TransformResult(silverPath, bronzeRows, written.count(), checksum(written))
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
