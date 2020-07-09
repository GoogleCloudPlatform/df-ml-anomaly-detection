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
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("serial")
public class MergeLogAggrMap extends SimpleFunction<Row, Row> {
  private static final Logger LOG = LoggerFactory.getLogger(MergeLogAggrMap.class);

  @Override
  public Row apply(Row input) {

    Row aggrRow =
        Row.withSchema(Util.bqLogSchema)
            .addValues(
                input.getRow("key").getString("subscriberId"),
                input.getRow("key").getString("dstSubnet"),
                Util.getTimeStamp(),
                input.getRow("value").getInt64("number_of_records").intValue(),
                input.getRow("value").getInt64("number_of_unique_ips").intValue(),
                input.getRow("value").getInt64("number_of_unique_ports").intValue(),
                input.getRow("value").getInt32("max_tx_bytes"),
                input.getRow("value").getInt32("min_tx_bytes"),
                input.getRow("value").getDouble("avg_tx_bytes"),
                input.getRow("value").getInt32("max_rx_bytes"),
                input.getRow("value").getInt32("min_rx_bytes"),
                input.getRow("value").getDouble("avg_rx_bytes"),
                input.getRow("value").getInt32("max_duration"),
                input.getRow("value").getInt32("min_duration"),
                input.getRow("value").getDouble("avg_duration"))
            .build();
    LOG.debug("Aggr Row {}", aggrRow.toString());
    return aggrRow;
  }
}
