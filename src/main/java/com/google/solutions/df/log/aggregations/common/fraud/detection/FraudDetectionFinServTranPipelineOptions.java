/*
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.solutions.df.log.aggregations.common.fraud.detection;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;

public interface FraudDetectionFinServTranPipelineOptions extends DataflowPipelineOptions {
  @Description("Subscriber Id to receive message from")
  String getSubscriberId();

  void setSubscriberId(String value);

  @Description("BQ Write Method")
  @Default.Enum("DEFAULT")
  BigQueryIO.Write.Method getWriteMethod();

  void setWriteMethod(BigQueryIO.Write.Method value);

  @Description("Probability For Prediction 0.99 means 99%")
  @Default.Double(0.99)
  Double getProbability();

  void setProbability(Double value);

  @Description("Batch Insert Trigger Frequency defaulted to 1 min")
  @Default.Integer(2)
  Integer getBatchFrequency();

  void setBatchFrequency(Integer value);

  @Description("Transaction Table- Must exist as partition table")
  String getTableSpec();

  void setTableSpec(String value);

  @Description("Aggregation Window Interval Default to 60 seconds")
  @Default.Integer(5)
  Integer getWindowInterval();

  void setWindowInterval(Integer value);

  @Description("BQ Fraud Transaction Storage- Must exist as partition table")
  String getOutlierTableSpec();

  void setOutlierTableSpec(String value);

  @Description("GCS temp location for BQ write")
  String getCustomGcsTempLocation();

  void setCustomGcsTempLocation(String value);

  @Description("input file pattern- json files")
  String getInputFilePattern();

  void setInputFilePattern(String value);

  @Description("Batch Size in Bytes")
  @Default.Integer(50000)
  Integer getBatchSize();

  void setBatchSize(Integer value);

  @Description("Model id")
  String getModelId();

  void setModelId(String value);

  @Description("Version id")
  String getVersionId();

  void setVersionId(String value);

  @Description("Key Range")
  @Default.Integer(100)
  Integer getKeyRange();

  void setKeyRange(Integer value);
}
