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

import com.google.auto.value.AutoValue;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonSyntaxException;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.JsonToRow;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Watch.Growth;
import org.apache.beam.sdk.transforms.WithTimestamps;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTagList;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@AutoValue
public abstract class ReadTransactionTransform extends PTransform<PBegin, PCollection<Row>> {
  public static final Logger LOG = LoggerFactory.getLogger(ReadTransactionTransform.class);

  public abstract String subscriber();

  public abstract String filePattern();

  public abstract Duration pollInterval();

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setSubscriber(String topic);

    public abstract Builder setFilePattern(String filePattern);

    public abstract Builder setPollInterval(Duration pollInterval);

    public abstract ReadTransactionTransform build();
  }

  public static Builder newBuilder() {
    return new AutoValue_ReadTransactionTransform.Builder();
  }

  @Override
  public PCollection<Row> expand(PBegin input) {

    PCollection<String> fileRow =
        input
            .apply(
                "ReadFromGCS",
                TextIO.read().from(filePattern()).watchForNewFiles(pollInterval(), Growth.never()))
            .apply("AssignEventTimestamp", WithTimestamps.of((String rec) -> Instant.now()));

    PCollection<String> pubsubMessage =
        input.apply("ReadFromPubSub", PubsubIO.readStrings().fromSubscription(subscriber()));

    return PCollectionList.of(fileRow)
        .and(pubsubMessage)
        .apply(Flatten.<String>pCollections())
        .apply(
            "ValidateJson",
            ParDo.of(new JsonValidatorFn())
                .withOutputTags(Util.successJsonTag, TupleTagList.of(Util.failedJsonTag)))
        .get(Util.successJsonTag)
        .apply("JsonToRow", JsonToRow.withSchema(Util.transactionSchema));
  }

  public static class JsonValidatorFn extends DoFn<String, String> {
    public Gson gson;
    private final Counter numberOfTransProcessed =
        Metrics.counter(ReadTransactionTransform.class, "numberOfTransProcessed");

    @Setup
    public void setup() {
      gson = new Gson();
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      String input = c.element();
      try {
        JsonObject convertedObject = gson.fromJson(input, JsonObject.class);
        numberOfTransProcessed.inc();
        c.output(Util.successJsonTag, convertedObject.toString());
        LOG.debug("log: {}", convertedObject.toString());
      } catch (JsonSyntaxException ex) {
        c.output(Util.failedJsonTag, input);
        LOG.error("Error Parsing Json {}", ex.getMessage());
      }
    }
  }
}
