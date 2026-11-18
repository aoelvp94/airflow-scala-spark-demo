package com.mycompany.airflow.hello;

/** A Java bean with properties, used as a control XCom value. */
public class ScoreSummaryBean {
  private final String region;
  private final double threshold;

  public ScoreSummaryBean(String region, double threshold) {
    this.region = region;
    this.threshold = threshold;
  }

  public String getRegion() {
    return region;
  }

  public double getThreshold() {
    return threshold;
  }
}
