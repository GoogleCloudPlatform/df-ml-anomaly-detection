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
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConvertRowToNetworkLog extends SimpleFunction<Row, NetworkLog> {
  public static final Logger LOG = LoggerFactory.getLogger(ConvertRowToNetworkLog.class);

  @Override
  public NetworkLog apply(Row row) {
    // row.getString("subscriberId")
    NetworkLog log =
        NetworkLog.newBuilder()
            .setSubscriberId(row.getString("subscriberId"))
            .setSrcIP(row.getString("srcIP"))
            .setDstIP(row.getString("dstIP"))
            .setSrcPort(row.getInt64("srcPort"))
            .setDstPort(row.getInt64("dstPort"))
            .setTxBytes(row.getInt64("txBytes"))
            .setRxBytes(row.getInt64("rxBytes"))
            .setStartTime(row.getInt64("startTime"))
            .setEndTime(row.getInt64("endTime"))
            .setTcpFlag(row.getInt64("tcpFlag"))
            .setProtocolName(row.getString("protocolName"))
            .setProtocolNumber(row.getInt64("protocolNumber"))
            .build();
    LOG.info(log.toString());
    return log;
  }
}
