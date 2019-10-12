package com.google.solutions.df.log.aggregations.common;

import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogAggrMapElement extends SimpleFunction<Row, Row> {
  private static final Logger LOG = LoggerFactory.getLogger(LogAggrMapElement.class);

  @Override
  public Row apply(Row row) {

    String dstSubnet = Util.findSubnet(row.getString("dstIP"));
    Integer duration = Util.findDuration(row.getInt64("startTime"), row.getInt64("endTime"));

    return Row.withSchema(row.getSchema())
        .addValues(
            row.getString("subscriberId"),
            row.getString("srcIP"),
            row.getString("dstIP"),
            row.getInt32("srcPort"),
            row.getInt32("dstPort"),
            row.getInt32("txBytes"),
            row.getInt32("rxBytes"),
            row.getInt64("startTime"),
            row.getInt64("endTime"),
            row.getInt32("tcpFlag"),
            row.getString("protocolName"),
            row.getInt32("protocolNumber"),
            dstSubnet,
            duration)
        .build();
  }
}
