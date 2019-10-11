package com.google.solutions.df.log.aggregations.common;

import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.Row;

public class LogAggrMapElement extends SimpleFunction<Row, Row> {

  @Override
  public Row apply(Row row) {

    String dstSubnet = Util.findSubnet(row.getString("dstIP"));
    Long duration =
        Util.findDuration(row.getInt64("startTime"), row.getInt64("endTime")).longValue();
    return Row.withSchema(Util.networkLogAggrSchema)
        .addValue(row.getString("subscriberId"))
        .addValue(row.getString("srcIP"))
        .addValue(dstSubnet)
        .addValue(row.getInt64("srcPort"))
        .addValue(row.getInt64("dstPort"))
        .addValue(row.getInt64("txBytes"))
        .addValue(row.getInt64("rxBytes"))
        .addValue(duration)
        .addValue(row.getInt32("tcpFlag"))
        .addValue(row.getString("protocolName"))
        .addValue(row.getInt32("protocolNumber"))
        .build();
  }
}
