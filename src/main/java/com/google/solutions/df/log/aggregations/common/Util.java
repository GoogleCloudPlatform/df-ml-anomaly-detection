package com.google.solutions.df.log.aggregations.common;

import static org.apache.beam.sdk.schemas.Schema.toSchema;

import java.util.stream.Stream;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.commons.net.util.SubnetUtils;
import org.joda.time.Period;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Util {
  public static final Logger LOG = LoggerFactory.getLogger(Util.class);
  private static final String maskString = "255.255.255.252";
  public static final Schema networkLogSchema =
      Stream.of(
              Schema.Field.of("subscriberId", FieldType.STRING).withNullable(true),
              Schema.Field.of("srcIP", FieldType.STRING).withNullable(true),
              Schema.Field.of("dstIP", FieldType.STRING).withNullable(true),
              Schema.Field.of("srcPort", FieldType.STRING).withNullable(true),
              Schema.Field.of("dstPort", FieldType.STRING).withNullable(true),
              Schema.Field.of("txBytes", FieldType.INT64).withNullable(true),
              Schema.Field.of("rxBytes", FieldType.INT64).withNullable(true),
              Schema.Field.of("startTime", FieldType.INT64).withNullable(true),
              Schema.Field.of("endTime", FieldType.INT64).withNullable(true),
              Schema.Field.of("tcpFlag", FieldType.INT32).withNullable(true),
              Schema.Field.of("protocolName", FieldType.STRING).withNullable(true),
              Schema.Field.of("protocolNumber", FieldType.INT32).withNullable(true))
          .collect(toSchema());

  public static final Schema networkLogAggrSchema =
      Stream.of(
              Schema.Field.of("subscriberId", FieldType.STRING).withNullable(true),
              Schema.Field.of("srcIP", FieldType.STRING).withNullable(true),
              Schema.Field.of("dstSubnet", FieldType.STRING).withNullable(true),
              Schema.Field.of("srcPort", FieldType.STRING).withNullable(true),
              Schema.Field.of("dstPort", FieldType.STRING).withNullable(true),
              Schema.Field.of("txBytes", FieldType.INT64).withNullable(true),
              Schema.Field.of("rxBytes", FieldType.INT64).withNullable(true),
              Schema.Field.of("duration", FieldType.INT64).withNullable(true),
              Schema.Field.of("tcpFlag", FieldType.INT32).withNullable(true),
              Schema.Field.of("protocolName", FieldType.STRING).withNullable(true),
              Schema.Field.of("protocolNumber", FieldType.INT32).withNullable(true))
          .collect(toSchema());
  public static final Schema bQTableSchema =
      Stream.of(
              Schema.Field.of("transaction_timestamp", FieldType.INT64).withNullable(true),
              Schema.Field.of("num_of_unique_ips", FieldType.INT32).withNullable(true),
              Schema.Field.of("num_of_unique_ports", FieldType.INT32).withNullable(true),
              Schema.Field.of("num_of_records", FieldType.INT32).withNullable(true),
              Schema.Field.of("max_tx_bytes", FieldType.INT64).withNullable(true),
              Schema.Field.of("min_tx_bytes", FieldType.INT64).withNullable(true),
              Schema.Field.of("avg_tx_bytes", FieldType.INT64).withNullable(true),
              Schema.Field.of("max_rx_bytes", FieldType.INT64).withNullable(true),
              Schema.Field.of("min_rx_bytes", FieldType.INT64).withNullable(true),
              Schema.Field.of("max_duration", FieldType.INT64).withNullable(true),
              Schema.Field.of("min_duration", FieldType.INT64).withNullable(true),
              Schema.Field.of("avg_duration", FieldType.INT64).withNullable(true))
          .collect(toSchema());

  public static String findSubnet(String dstIP) {
    return new SubnetUtils(dstIP, maskString).getInfo().getCidrSignature();
  }

  public static Integer findDuration(long startTime, long endTime) {
    return Math.abs(new Period(endTime, startTime).getMillis());
  }
}
