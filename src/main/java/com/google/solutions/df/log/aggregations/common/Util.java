package com.google.solutions.df.log.aggregations.common;

import static org.apache.beam.sdk.schemas.Schema.toSchema;

import java.util.stream.Stream;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.commons.net.util.SubnetUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Instant;
import org.joda.time.Period;
import org.joda.time.PeriodType;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Util {
  public static final Logger LOG = LoggerFactory.getLogger(Util.class);
  private static final DateTimeFormatter TIMESTAMP_FORMATTER =
      DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");
  private static final String maskString = "255.255.255.252";
  public static final Schema networkLogSchema =
      Stream.of(
              Schema.Field.of("subscriberId", FieldType.STRING).withNullable(true),
              Schema.Field.of("srcIP", FieldType.STRING).withNullable(true),
              Schema.Field.of("dstIP", FieldType.STRING).withNullable(true),
              Schema.Field.of("srcPort", FieldType.INT32).withNullable(true),
              Schema.Field.of("dstPort", FieldType.INT32).withNullable(true),
              Schema.Field.of("txBytes", FieldType.INT32).withNullable(true),
              Schema.Field.of("rxBytes", FieldType.INT32).withNullable(true),
              Schema.Field.of("startTime", FieldType.INT64).withNullable(true),
              Schema.Field.of("endTime", FieldType.INT64).withNullable(true),
              Schema.Field.of("tcpFlag", FieldType.INT32).withNullable(true),
              Schema.Field.of("protocolName", FieldType.STRING).withNullable(true),
              Schema.Field.of("protocolNumber", FieldType.INT32).withNullable(true))
          .collect(toSchema());

  public static final Schema bqLogSchema =
      Stream.of(
              Schema.Field.of("subscriber_id", FieldType.STRING).withNullable(true),
              Schema.Field.of("dst_subnet", FieldType.STRING).withNullable(true),
              Schema.Field.of("transaction_time", FieldType.STRING).withNullable(true),
              Schema.Field.of("number_of_records", FieldType.INT32).withNullable(true),
              Schema.Field.of("number_of_ips", FieldType.INT32).withNullable(true),
              Schema.Field.of("number_of_ports", FieldType.INT32).withNullable(true),
              Schema.Field.of("max_tx_bytes", FieldType.INT32).withNullable(true),
              Schema.Field.of("min_tx_bytes", FieldType.INT32).withNullable(true),
              Schema.Field.of("avg_tx_bytes", FieldType.DOUBLE).withNullable(true),
              Schema.Field.of("max_rx_bytes", FieldType.INT32).withNullable(true),
              Schema.Field.of("min_rx_bytes", FieldType.INT32).withNullable(true),
              Schema.Field.of("avg_rx_bytes", FieldType.DOUBLE).withNullable(true),
              Schema.Field.of("max_duration", FieldType.INT32).withNullable(true),
              Schema.Field.of("min_duration", FieldType.INT32).withNullable(true),
              Schema.Field.of("avg_duration", FieldType.DOUBLE).withNullable(true))
          .collect(toSchema());

  public static String findSubnet(String dstIP) {
    return new SubnetUtils(dstIP, maskString).getInfo().getCidrSignature();
  }

  public static Integer findDuration(long startTime, long endTime) {

    DateTime start = new DateTime(startTime);
    DateTime end = new DateTime(endTime);
    Period period = new Period(start, end, PeriodType.millis());
    return Math.abs(period.getMillis());
  }

  public static String getTimeStamp() {
    return TIMESTAMP_FORMATTER.print(Instant.now().toDateTime(DateTimeZone.UTC));
  }
}
