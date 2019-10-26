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

import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.Row;

public class MergeLogAggrMap extends SimpleFunction<KV<Row, Row>, Row> {
  @Override
  public Row apply(KV<Row, Row> input) {

    return Row.withSchema(Util.bqLogSchema)
        .addValues(
            input.getKey().getString("subscriberId"),
            input.getKey().getString("dstSubnet"),
            Util.getTimeStamp(),
            input.getValue().getInt64("number_of_records").intValue(),
            input.getValue().getInt64("number_of_unique_ips").intValue(),
            input.getValue().getInt64("number_of_unique_ports").intValue(),
            input.getValue().getInt32("max_tx_bytes"),
            input.getValue().getInt32("min_tx_bytes"),
            input.getValue().getDouble("avg_tx_bytes"),
            input.getValue().getInt32("max_rx_bytes"),
            input.getValue().getInt32("min_rx_bytes"),
            input.getValue().getDouble("avg_rx_bytes"),
            input.getValue().getInt32("max_duration"),
            input.getValue().getInt32("min_duration"),
            input.getValue().getDouble("avg_duration"))
        .build();
  }
}
