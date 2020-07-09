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
  public static TupleTag<String> failedJsonTag = new TupleTag<String>() {};
  public static TupleTag<String> successJsonTag = new TupleTag<String>() {};
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
