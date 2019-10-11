/*
 * Copyright 2019 Google LLC
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
 * <p>To execute this pipeline locally, specify following param
 *
 * <pre>{@code
 * --subcriberId=projects/[project-id]/subscriptions/[sub-id]
 * }</pre>
 *
 * <pre>{@code
 * --writeMethod=FILE_LOAD
 * }</pre>
 *
 * <pre>{@code
 * --batchFrequency=2
 * }</pre>
 *
 * <pre>{@code
 * --tableSpec=<project_id>:<dataset_id>.<table_id>
 * }</pre>
 *
 * <pre>{@code
 * --windowInterval=5
 * }</pre>
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

  @Description("BQ Table Spec- Must exist as partition table")
  String getTableSpec();

  void setTableSpec(String value);

  @Description("Aggregation Window Interval")
  @Default.Integer(1)
  Integer getWindowInterval();

  void setWindowInterval(Integer value);
}
