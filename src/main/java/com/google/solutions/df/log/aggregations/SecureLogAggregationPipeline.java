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

import com.google.solutions.df.log.aggregations.common.AvgFn;
import com.google.solutions.df.log.aggregations.common.SecureLogAggregationPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.transforms.Group;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.JsonToRow;
import org.apache.beam.sdk.transforms.Max;
import org.apache.beam.sdk.transforms.Min;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SecureLogAggregationPipeline {
  public static final Logger LOG = LoggerFactory.getLogger(SecureLogAggregationPipeline.class);
  public static final Schema networkLogSchema =
      Schema.builder()
          .addStringField("subscriberId")
          .addStringField("srcIP")
          .addStringField("dstIP")
          .addInt64Field("srcPort")
          .addInt64Field("dstPort")
          .addInt64Field("txBytes")
          .addInt64Field("rxBytes")
          .addInt64Field("startTime")
          .addInt64Field("endTime")
          .addInt64Field("tcpFlag")
          .addStringField("protocolName")
          .addInt64Field("protocolNumber")
          .build();
  public static final Schema networkLogAggregationSchema =
      Schema.builder()
          .addInt64Field("numberOfUniqueIPs")
          .addInt64Field("numberOfUniquePorts")
          .addDoubleField("avgTxBytes")
          .addInt64Field("minTxBytes")
          .addInt64Field("maxTxBytes")
          .build();

  public static void main(String args[]) {

    SecureLogAggregationPipelineOptions options =
        PipelineOptionsFactory.fromArgs(args)
            .withValidation()
            .as(SecureLogAggregationPipelineOptions.class);
    run(options);
  }

  public static PipelineResult run(SecureLogAggregationPipelineOptions options) {

    Pipeline p = Pipeline.create(options);

    PCollection<KV<Row, Row>> logData =
        p.apply(PubsubIO.readStrings().fromSubscription(options.getSubscriberId()))
            .apply(JsonToRow.withSchema(networkLogSchema))
            .setRowSchema(networkLogSchema)
            .apply(
                "Fixed Window",
                Window.<Row>into(
                        FixedWindows.of(Duration.standardMinutes(options.getWindowInterval())))
                    .triggering(
                        AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.ZERO))
                    .discardingFiredPanes()
                    .withAllowedLateness(Duration.ZERO))
            .apply(
                "Group By Sub Id & DestIP",
                Group.<Row>byFieldNames("subscriberId", "dstIP")
                    .aggregateField("srcIP", Count.combineFn(), "numberOfUniqueIPs")
                    .aggregateField("srcPort", Count.combineFn(), "numberOfUniquePorts")
                    .aggregateField("txBytes", new AvgFn(), "avgTxByes")
                    .aggregateField("txBytes", Max.ofLongs(), "maxTxByes")
                    .aggregateField("txBytes", Min.ofLongs(), "minTxByes"));

    logData.apply(
        "Print",
        ParDo.of(
            new DoFn<KV<Row, Row>, String>() {
              @ProcessElement
              public void processElement(ProcessContext c) {
                Row row = c.element().getValue();
                LOG.info("row value {}", row.toString());
              }
            }));

    return p.run();
  }
}
