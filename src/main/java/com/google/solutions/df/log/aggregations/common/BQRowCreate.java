package com.google.solutions.df.log.aggregations.common;

import static org.apache.beam.sdk.schemas.Schema.toSchema;

import java.util.stream.Stream;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.Row;

public class BQRowCreate extends SimpleFunction<KV<Row, Row>, Row> {

  public Row apply(KV<Row, Row> input) {

    Schema bqRowType =
        Stream.of(
                Schema.Field.of("subscriber_id", Schema.FieldType.INT32),
                Schema.Field.of("dst_subnet", Schema.FieldType.STRING),
                Schema.Field.of("transaction_time", Schema.FieldType.STRING),
                Schema.Field.of("vector", Schema.FieldType.row(input.getValue().getSchema())))
            .collect(toSchema());

    return Row.withSchema(bqRowType)
        .addValues(
            input.getKey().getInt32("subscriberId"),
            input.getKey().getString("dstSubnet"),
            Util.getTimeStamp(),
            input.getValue())
        .build();
  }
}
