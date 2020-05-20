package com.google.solutions.df.log.aggregations.common.fraud.detection;

import static org.apache.beam.sdk.schemas.Schema.toSchema;

import java.util.stream.Stream;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Util {

  public static final Logger LOG = LoggerFactory.getLogger(Util.class);

  public static TupleTag<Row> featureDataTag = new TupleTag<Row>() {};
  public static TupleTag<String> predictDataTag = new TupleTag<String>() {};
  public static final Integer keyRange = 100;
  public static final Schema prerdictonOutputSchema =
      Stream.of(
              Schema.Field.of("transactionId", FieldType.STRING).withNullable(true),
              Schema.Field.of("logistic", FieldType.DOUBLE).withNullable(true),
              Schema.Field.of("json_response", FieldType.STRING).withNullable(true))
          .collect(toSchema());

  public static final Schema prerdictonInputSchema =
      Stream.of(
              Schema.Field.of("step", FieldType.INT32).withNullable(true),
              Schema.Field.of("type", FieldType.STRING).withNullable(true),
              Schema.Field.of("amount", FieldType.DOUBLE).withNullable(true),
              Schema.Field.of("oldbalanceOrg", FieldType.DOUBLE).withNullable(true),
              Schema.Field.of("newbalanceOrig", FieldType.DOUBLE).withNullable(true),
              Schema.Field.of("oldbalanceDest", FieldType.DOUBLE).withNullable(true),
              Schema.Field.of("newbalanceDest", FieldType.DOUBLE).withNullable(true),
              Schema.Field.of("transactionId", FieldType.STRING).withNullable(true))
          .collect(toSchema());
  public static final Schema transactionSchema =
      Stream.of(
              Schema.Field.of("step", FieldType.INT32).withNullable(true),
              Schema.Field.of("nameOrig", FieldType.STRING).withNullable(true),
              Schema.Field.of("nameDest", FieldType.STRING).withNullable(true),
              Schema.Field.of("isFlaggedFraud", FieldType.INT32).withNullable(true),
              Schema.Field.of("isFraud", FieldType.INT32).withNullable(true),
              Schema.Field.of("type", FieldType.STRING).withNullable(true),
              Schema.Field.of("amount", FieldType.DOUBLE).withNullable(true),
              Schema.Field.of("oldbalanceOrg", FieldType.DOUBLE).withNullable(true),
              Schema.Field.of("newbalanceOrig", FieldType.DOUBLE).withNullable(true),
              Schema.Field.of("oldbalanceDest", FieldType.DOUBLE).withNullable(true),
              Schema.Field.of("newbalanceDest", FieldType.DOUBLE).withNullable(true),
              Schema.Field.of("transactionId", FieldType.STRING).withNullable(true))
          .collect(toSchema());
}
