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

import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.Row;

@SuppressWarnings("serial")
public class MergeOutlierMap extends SimpleFunction<KV<Row, Row>, Row> {
  @Override
  public Row apply(KV<Row, Row> input) {
    return Row.withSchema(Util.outlierSchema)
        .addValues(
            input.getKey().getString("subscriber_id"),
            input.getKey().getString("dst_subnet"),
            input.getKey().getString("transaction_time"),
            input.getKey().getInt32("number_of_unique_ips"),
            input.getKey().getInt32("number_of_unique_ports"),
            input.getKey().getInt32("number_of_records"),
            input.getKey().getInt32("max_tx_bytes"),
            input.getKey().getInt32("min_tx_bytes"),
            input.getKey().getDouble("avg_tx_bytes"),
            input.getKey().getInt32("max_rx_bytes"),
            input.getKey().getInt32("min_rx_bytes"),
            input.getKey().getDouble("avg_rx_bytes"),
            input.getKey().getInt32("max_duration"),
            input.getKey().getInt32("min_duration"),
            input.getKey().getDouble("avg_duration"),
            input.getKey().getInt32("centroid_id"),
            input.getKey().getInt64("centroid_radius"),
            input.getValue().getInt64("nearest_distance_from_centroid"))
        .build();
  }
}
