package com.mycompany.airflow.hello;

import org.apache.airflow.sdk.TaskInput;

public class ScoreInput implements TaskInput {
  public String region;
  public double threshold;
}
