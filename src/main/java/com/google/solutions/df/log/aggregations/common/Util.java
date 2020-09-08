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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.beam.sdk.schemas.Schema.toSchema;

import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.common.collect.ImmutableList;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.beam.sdk.extensions.gcp.util.gcsfs.GcsPath;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.commons.math3.stat.descriptive.moment.StandardDeviation;
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

@SuppressWarnings("serial")
public class Util {
  public static final Logger LOG = LoggerFactory.getLogger(Util.class);
  public static TupleTag<String> failureTag = new TupleTag<String>() {};
  public static TupleTag<String> successTag = new TupleTag<String>() {};

  public static final String DAY_PARTITION = "DAY";
  public static final Integer NUM_OF_SHARDS = 100;

  private static final DateTimeFormatter TIMESTAMP_FORMATTER =
      DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");
  private static final String maskString = "255.255.252.0";
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
  public static final Schema networkLogSchemaWithGeo =
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
              Schema.Field.of("protocolNumber", FieldType.INT32).withNullable(true),
              Schema.Field.of("geoCountry", FieldType.STRING).withNullable(true),
              Schema.Field.of("geoCity", FieldType.STRING).withNullable(true),
              Schema.Field.of("latitude", FieldType.DOUBLE).withNullable(true),
              Schema.Field.of("longitude", FieldType.DOUBLE).withNullable(true))
          .collect(toSchema());

  public static final Schema bqLogSchema =
      Stream.of(
              Schema.Field.of("subscriber_id", FieldType.STRING).withNullable(true),
              Schema.Field.of("dst_subnet", FieldType.STRING).withNullable(true),
              Schema.Field.of("transaction_time", FieldType.STRING).withNullable(true),
              Schema.Field.of("number_of_records", FieldType.INT32).withNullable(true),
              Schema.Field.of("number_of_unique_ips", FieldType.INT32).withNullable(true),
              Schema.Field.of("number_of_unique_ports", FieldType.INT32).withNullable(true),
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
  public static final Schema outlierSchema =
      Stream.of(
              Schema.Field.of("subscriber_id", FieldType.STRING).withNullable(true),
              Schema.Field.of("dst_subnet", FieldType.STRING).withNullable(true),
              Schema.Field.of("transaction_time", FieldType.STRING).withNullable(true),
              Schema.Field.of("number_of_records", FieldType.INT32).withNullable(true),
              Schema.Field.of("number_of_unique_ips", FieldType.INT32).withNullable(true),
              Schema.Field.of("number_of_unique_ports", FieldType.INT32).withNullable(true),
              Schema.Field.of("max_tx_bytes", FieldType.INT32).withNullable(true),
              Schema.Field.of("min_tx_bytes", FieldType.INT32).withNullable(true),
              Schema.Field.of("avg_tx_bytes", FieldType.DOUBLE).withNullable(true),
              Schema.Field.of("max_rx_bytes", FieldType.INT32).withNullable(true),
              Schema.Field.of("min_rx_bytes", FieldType.INT32).withNullable(true),
              Schema.Field.of("avg_rx_bytes", FieldType.DOUBLE).withNullable(true),
              Schema.Field.of("max_duration", FieldType.INT32).withNullable(true),
              Schema.Field.of("min_duration", FieldType.INT32).withNullable(true),
              Schema.Field.of("avg_duration", FieldType.DOUBLE).withNullable(true),
              Schema.Field.of("centroid_id", FieldType.INT32).withNullable(true).withNullable(true))
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

  public static String getClusterDetails(String gcsPath) {
    GcsPath path = GcsPath.fromUri(URI.create(gcsPath));
    Storage storage = StorageOptions.getDefaultInstance().getService();
    BlobId blobId = BlobId.of(path.getBucket(), path.getObject());
    byte[] content = storage.readAllBytes(blobId);
    String contentString = new String(content, UTF_8);
    LOG.debug("Query: {}", contentString);
    return contentString;
  }

  public static double[] getAggrVector(Row aggrData) {
    return ImmutableList.of(
            aggrData.getInt32("number_of_records").doubleValue(),
            aggrData.getInt32("max_tx_bytes").doubleValue(),
            aggrData.getInt32("min_tx_bytes").doubleValue(),
            aggrData.getDouble("avg_tx_bytes"),
            aggrData.getInt32("max_rx_bytes").doubleValue(),
            aggrData.getInt32("min_rx_bytes").doubleValue(),
            aggrData.getDouble("avg_rx_bytes").doubleValue(),
            aggrData.getInt32("max_duration").doubleValue(),
            aggrData.getInt32("min_duration").doubleValue(),
            aggrData.getDouble("avg_duration").doubleValue())
        .stream()
        .mapToDouble(d -> d)
        .toArray();
  }

  public static double calculateStdDeviation(double[] inputVector, double[] featureVector) {

    double[] differences =
        IntStream.range(0, inputVector.length)
            .mapToDouble(i -> inputVector[i] - featureVector[i])
            .toArray();
    StandardDeviation sd = new StandardDeviation();
    return sd.evaluate(differences);
  }

  public static String randomKeyGenerator(Integer range) {
    return String.valueOf(new Random().nextInt(range * 100));
  }

  public static List<String> getRawTableClusterFields() {
    return Arrays.asList("geoCountry", "geoCity");
  }

  public static List<String> getFeatureTableClusterFields() {
    return Arrays.asList("dst_subnet", "subscriber_id");
  }

  public static Long currentStartTime() {
    return Instant.now().toDateTime(DateTimeZone.UTC).getMillis();
  }

  public static Long currentEndTime() {
    return currentStartTime() + TimeUnit.MILLISECONDS.toMillis(new Random().nextInt(60));
  }
}
