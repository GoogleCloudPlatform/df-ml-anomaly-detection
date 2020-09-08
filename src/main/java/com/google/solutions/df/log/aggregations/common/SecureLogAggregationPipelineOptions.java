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
package com.google.solutions.df.log.aggregations.common;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;

/**
 * Pipeline options
 *
 * <p>To execute this pipeline locally, specify following params
 */
public interface SecureLogAggregationPipelineOptions extends DataflowPipelineOptions {
  @Description("Subscriber Id to receive message from")
  String getSubscriberId();

  void setSubscriberId(String value);

  @Description("BQ Write Method")
  @Default.Enum("DEFAULT")
  BigQueryIO.Write.Method getWriteMethod();

  void setWriteMethod(BigQueryIO.Write.Method value);

  @Description("Batch Insert Trigger Frequency defaulted to 1 min")
  @Default.Integer(1)
  Integer getBatchFrequency();

  void setBatchFrequency(Integer value);

  @Description("BQ Aggr Table Spec- Must exist as partition table")
  String getTableSpec();

  void setTableSpec(String value);

  @Description("Aggregation Window Interval Default to 60 seconds")
  @Default.Integer(60)
  Integer getWindowInterval();

  void setWindowInterval(Integer value);

  @Description("GCS Path for the Cluster Query")
  String getClusterQuery();

  void setClusterQuery(String value);

  @Description("BQ Outlier Table Spec - Must exist before the run")
  String getOutlierTableSpec();

  void setOutlierTableSpec(String value);

  @Description("BQ Raw Log Data Table Spec- Must exist as partition table")
  String getLogTableSpec();

  void setLogTableSpec(String value);

  @Description("GCS temp location for BQ write")
  String getCustomGcsTempLocation();

  void setCustomGcsTempLocation(String value);

  @Description("input file pattern - json files")
  String getInputFilePattern();

  void setInputFilePattern(String value);

  @Description("DLP batch size")
  @Default.Integer(500)
  Integer getBatchSize();

  void setBatchSize(Integer value);

  @Description("Ingestion Rate")
  @Default.Integer(250000)
  Integer getIngestionRate();

  void setIngestionRate(Integer value);

  @Description("DLP Inspect Tempalte Name")
  String getInspectTemplateName();

  void setInspectTemplateName(String value);

  @Description("DLP Deid Tempalte Name")
  String getDeidTemplateName();

  void setDeidTemplateName(String value);

  @Description("GCS path for geo lite db")
  @Default.String("gs://df-ml-anomaly-detection-mock-data/GeoLite2-City.mmdb")
  String getGeoDbpath();

  void setGeoDbpath(String value);
}
