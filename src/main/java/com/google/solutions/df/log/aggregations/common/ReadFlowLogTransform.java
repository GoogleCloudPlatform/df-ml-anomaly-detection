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

import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.Watch.Growth;
import org.apache.beam.sdk.transforms.WithTimestamps;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@AutoValue
@SuppressWarnings("serial")
public abstract class ReadFlowLogTransform extends PTransform<PBegin, PCollection<Row>> {
  public static final Logger LOG = LoggerFactory.getLogger(ReadFlowLogTransform.class);

  public abstract String subscriber();

  public abstract String filePattern();

  public abstract Duration pollInterval();

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setSubscriber(String topic);

    public abstract Builder setFilePattern(String filePattern);

    public abstract Builder setPollInterval(Duration pollInterval);

    public abstract ReadFlowLogTransform build();
  }

  public static Builder newBuilder() {
    return new AutoValue_ReadFlowLogTransform.Builder();
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
        .apply("FlowLogs Convert To Rows", new JsonToRowValidationTransform());
  }
}
