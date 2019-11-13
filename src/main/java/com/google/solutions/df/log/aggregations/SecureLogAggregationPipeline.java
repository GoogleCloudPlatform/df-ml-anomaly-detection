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
import com.google.solutions.df.log.aggregations.common.CentroidVector;
import com.google.solutions.df.log.aggregations.common.ClusterDataMapElement;
import com.google.solutions.df.log.aggregations.common.JsonToRowValidationTransform;
import com.google.solutions.df.log.aggregations.common.LogRowTransform;
import com.google.solutions.df.log.aggregations.common.MergeLogAggrMap;
import com.google.solutions.df.log.aggregations.common.PredictTransform;
import com.google.solutions.df.log.aggregations.common.SecureLogAggregationPipelineOptions;
import com.google.solutions.df.log.aggregations.common.Util;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead.Method;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
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

    // side input as centroid id, radius and other features
    PCollectionView<List<CentroidVector>> centroidFeatures =
        p.apply(
                BigQueryIO.read(new ClusterDataMapElement())
                    .fromQuery(Util.getClusterDetails(options.getClusterQuery()))
                    .usingStandardSql()
                    .withMethod(Method.EXPORT))
            .apply("Centroid Data As Side Input", View.asList());

    PCollection<Row> rows =
        p.apply(
                "Read LOG Events",
                PubsubIO.readStrings().fromSubscription(options.getSubscriberId()))
            .apply("Input Converts To Row", new JsonToRowValidationTransform())
            .apply(
                "Fixed Window",
                Window.<Row>into(
                        FixedWindows.of(Duration.standardMinutes(options.getWindowInterval())))
                    .triggering(
                        AfterProcessingTime.pastFirstElementInPane()
                            .plusDelayOf(Duration.standardMinutes(options.getWindowInterval())))
                    .discardingFiredPanes()
                    .withAllowedLateness(Duration.ZERO))
            .apply("Log Aggregation Transform", new LogRowTransform())
            .apply("Merge Aggr Row", MapElements.via(new MergeLogAggrMap()))
            .setRowSchema(Util.bqLogSchema);

    rows.apply(
        "File Load to Aggr Table",
        BQWriteTransform.newBuilder()
            .setTableSpec(options.getTableSpec())
            .setBatchFrequency(options.getBatchFrequency())
            .setMethod(options.getWriteMethod())
            .setGcsTempLocation(StaticValueProvider.of(options.getCustomGcsTempLocation()))
            .build());

    // prediction - let's have some fun
    rows.apply(
            "Find Outliers",
            PredictTransform.newBuilder().setCentroidFeatureVector(centroidFeatures).build())
        .apply(
            "Streaming Insert To Outliers Table",
            BQWriteTransform.newBuilder()
                .setTableSpec(options.getOutlierTableSpec())
                .setMethod(BigQueryIO.Write.Method.STREAMING_INSERTS)
                .build());
    return p.run();
  }
}
