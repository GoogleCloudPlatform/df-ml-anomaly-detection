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

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonSyntaxException;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.JsonToRow;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.commons.validator.routines.InetAddressValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("serial")
public class JsonToRowValidationTransform
    extends PTransform<PCollection<String>, PCollection<Row>> {
  private static final Logger LOG = LoggerFactory.getLogger(JsonToRowValidationTransform.class);

  @Override
  public PCollection<Row> expand(PCollection<String> input) {

    PCollectionTuple output =
        input.apply(
            "Validated Json",
            ParDo.of(new JsonValidatorFn())
                .withOutputTags(Util.successTag, TupleTagList.of(Util.failureTag)));
    PCollection<Row> logRow =
        output
            .get(Util.successTag)
            .apply("Convert To Row", JsonToRow.withSchema(Util.networkLogSchema))
            .setRowSchema(Util.networkLogSchema);

    return logRow
        .apply(
            "ModifiedRow",
            ParDo.of(
                new DoFn<Row, Row>() {

                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    Row modifiedRow =
                        Row.fromRow(c.element())
                            .withFieldValue("startTime", Util.currentStartTime())
                            .withFieldValue("endTime", Util.currentEndTime())
                            .build();
                    c.output(modifiedRow);
                  }
                }))
        .setRowSchema(Util.networkLogSchema);
  }

  public static class JsonValidatorFn extends DoFn<String, String> {
    public Gson gson;

    @Setup
    public void setup() {
      gson = new Gson();
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      String input = c.element();
      LOG.debug("log: {}", input);
      try {
        JsonObject convertedObject = gson.fromJson(input, JsonObject.class);
        boolean validData =
            InetAddressValidator.getInstance()
                    .isValidInet4Address(convertedObject.get("dstIP").getAsString())
                && InetAddressValidator.getInstance()
                    .isValidInet4Address(convertedObject.get("srcIP").getAsString());

        if (validData) {
          c.output(convertedObject.toString());

        } else {
          String errMsg = String.format("Not a valid IP address %s", input);
          LOG.error(errMsg);
          c.output(Util.failureTag, errMsg);
        }
      } catch (JsonSyntaxException e) {
        c.output(Util.failureTag, e.getMessage());
      }
    }
  }
}
