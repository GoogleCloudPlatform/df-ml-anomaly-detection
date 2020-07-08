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

import com.google.common.collect.ImmutableList;
import org.apache.beam.sdk.io.gcp.bigquery.SchemaAndRecord;
import org.apache.beam.sdk.transforms.SerializableFunction;

@SuppressWarnings("serial")
public class ClusterDataMapElement
    implements SerializableFunction<SchemaAndRecord, CentroidVector> {

  @Override
  public CentroidVector apply(SchemaAndRecord input) {

    ImmutableList<Double> featureVector =
        ImmutableList.of(
            Double.parseDouble(input.getRecord().get("number_of_records").toString()),
            Double.parseDouble(input.getRecord().get("max_tx_bytes").toString()),
            Double.parseDouble(input.getRecord().get("min_tx_bytes").toString()),
            Double.parseDouble(input.getRecord().get("avg_tx_bytes").toString()),
            Double.parseDouble(input.getRecord().get("max_rx_bytes").toString()),
            Double.parseDouble(input.getRecord().get("min_rx_bytes").toString()),
            Double.parseDouble(input.getRecord().get("avg_rx_bytes").toString()),
            Double.parseDouble(input.getRecord().get("max_duration").toString()),
            Double.parseDouble(input.getRecord().get("min_duration").toString()),
            Double.parseDouble(input.getRecord().get("avg_duration").toString()));

    return CentroidVector.newBuilder()
        .setCentroidId(Integer.parseInt(input.getRecord().get("centroid_id").toString()))
        .setNormalizedDistance(
            Double.parseDouble(input.getRecord().get("normalized_dest").toString()))
        .setFeatureVectors(featureVector)
        .build();
  }
}
