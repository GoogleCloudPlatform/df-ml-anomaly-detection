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
package com.google.solutions.df.log.aggregations;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Description;

public interface SmartCityStreetLightAnalyticsPipelineOptions extends DataflowPipelineOptions {
  @Description("Subscriber Id to receive sensor from")
  String getSensorDataSub();

  void setSensorDataSub(String value);

  @Description("Subscriber Id to receive state message from")
  String getStateDataSub();

  void setStateDataSub(String value);

  @Description("BQ Sensor Table")
  String getSensorTableSpec();

  void setSensorTableSpec(String value);

  @Description("BQ State Table")
  String getStateTableSpec();

  void setStateTableSpec(String value);
}
