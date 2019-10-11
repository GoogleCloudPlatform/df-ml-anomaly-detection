/*
 * Copyright 2019 Google LLC
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

package com.google.solutions.df.log.aggregations;

import com.google.solutions.df.log.aggregations.common.BQWriteTransform;
import com.google.solutions.df.log.aggregations.common.LogRowTransform;
import com.google.solutions.df.log.aggregations.common.SecureLogAggregationPipelineOptions;
import com.google.solutions.df.log.aggregations.common.Util;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.JsonToRow;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SecureLogAggregationPipeline {
  public static final Logger LOG = LoggerFactory.getLogger(SecureLogAggregationPipeline.class);

  public static void main(String args[]) {

    SecureLogAggregationPipelineOptions options =
        PipelineOptionsFactory.fromArgs(args)
            .withValidation()
            .as(SecureLogAggregationPipelineOptions.class);
    run(options);
  }

  public static PipelineResult run(SecureLogAggregationPipelineOptions options) {

    Pipeline p = Pipeline.create(options);

    p.apply("Read LOG Events", PubsubIO.readStrings().fromSubscription(options.getSubscriberId()))
        .apply("Convert To Row", JsonToRow.withSchema(Util.networkLogSchema))
        .setRowSchema(Util.networkLogSchema)
        .apply(
            "Fixed Window",
            Window.<Row>into(FixedWindows.of(Duration.standardMinutes(options.getWindowInterval())))
                .triggering(
                    AfterProcessingTime.pastFirstElementInPane()
                        .plusDelayOf(Duration.standardMinutes(1)))
                .discardingFiredPanes()
                .withAllowedLateness(Duration.ZERO))
        .apply(new LogRowTransform())
        .apply(
            "BQ Write",
            BQWriteTransform.newBuilder()
                .setTableSpec(options.getTableSpec())
                .setBatchFrequency(options.getBatchFrequency())
                .setMethod(options.getWriteMethod())
                .build());

    return p.run();
  }
}
