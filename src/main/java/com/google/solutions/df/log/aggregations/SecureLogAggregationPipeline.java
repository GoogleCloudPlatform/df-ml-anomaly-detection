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
package com.google.solutions.df.log.aggregations;

import com.google.solutions.df.log.aggregations.common.BQWriteTransform;
import com.google.solutions.df.log.aggregations.common.CentroidVector;
import com.google.solutions.df.log.aggregations.common.ClusterDataMapElement;
import com.google.solutions.df.log.aggregations.common.DLPTransform;
import com.google.solutions.df.log.aggregations.common.LogRowTransform;
import com.google.solutions.df.log.aggregations.common.PredictTransform;
import com.google.solutions.df.log.aggregations.common.RawLogDataTransform;
import com.google.solutions.df.log.aggregations.common.ReadFlowLogTransform;
import com.google.solutions.df.log.aggregations.common.SecureLogAggregationPipelineOptions;
import com.google.solutions.df.log.aggregations.common.Util;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead.Method;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
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
  /** Default interval for polling files in GCS. */
  private static final Duration DEFAULT_POLL_INTERVAL = Duration.standardSeconds(30);

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
                "Latest Normalized Data",
                BigQueryIO.read(new ClusterDataMapElement())
                    .fromQuery(Util.getClusterDetails(options.getClusterQuery()))
                    .usingStandardSql()
                    .withMethod(Method.EXPORT))
            .apply("Centroids Data as Input", View.asList());
    // read from GCS and pub sub
    PCollection<Row> maybeTokenizedRows =
        p.apply(
            "Read FlowLog Data",
            ReadFlowLogTransform.newBuilder()
                .setFilePattern(options.getInputFilePattern())
                .setPollInterval(DEFAULT_POLL_INTERVAL)
                .setSubscriber(options.getSubscriberId())
                .build());
    // if passed, raw log data will be stored with country where IP is originated.
    if (options.getLogTableSpec() != null) {

      maybeTokenizedRows
          .apply(
              "ConvertIpToGeo",
              RawLogDataTransform.newBuilder().setDbPath(options.getGeoDbpath()).build())
          .setRowSchema(Util.networkLogSchemaWithGeo)
          .apply(
              "Batch to Log Table",
              BQWriteTransform.newBuilder()
                  .setTableSpec(options.getLogTableSpec())
                  .setBatchFrequency(options.getBatchFrequency())
                  .setMethod(options.getWriteMethod())
                  .setClusterFields(Util.getRawTableClusterFields())
                  .setGcsTempLocation(StaticValueProvider.of(options.getCustomGcsTempLocation()))
                  .build());
    }

    PCollection<Row> featureExtractedRows =
        maybeTokenizedRows
            .apply(
                "Fixed Window",
                Window.<Row>into(
                        FixedWindows.of(Duration.standardSeconds(options.getWindowInterval())))
                    .triggering(AfterWatermark.pastEndOfWindow())
                    .discardingFiredPanes()
                    .withAllowedLateness(Duration.ZERO))
            .apply("Feature Extraction", new LogRowTransform())
            .apply(
                "DLP Transformation",
                DLPTransform.newBuilder()
                    .setBatchSize(options.getBatchSize())
                    .setDeidTemplateName(options.getDeidTemplateName())
                    .setInspectTemplateName(options.getInspectTemplateName())
                    .setProjectId(options.getProject())
                    .setRandomKey(
                        Util.randomKeyGenerator(
                            Math.floorDiv(
                                options.getIngestionRate().intValue(), options.getMaxNumWorkers())))
                    .build())
            .setRowSchema(Util.bqLogSchema);

    featureExtractedRows.apply(
        "Batch to Feature Table",
        BQWriteTransform.newBuilder()
            .setTableSpec(options.getTableSpec())
            .setBatchFrequency(options.getBatchFrequency())
            .setMethod(options.getWriteMethod())
            .setClusterFields(Util.getFeatureTableClusterFields())
            .setGcsTempLocation(StaticValueProvider.of(options.getCustomGcsTempLocation()))
            .build());

    // prediction - let's have some fun
    featureExtractedRows
        .apply(
            "Anomaly Detection",
            PredictTransform.newBuilder().setCentroidFeatureVector(centroidFeatures).build())
        .apply(
            "Stream To Outlier Table",
            BQWriteTransform.newBuilder()
                .setTableSpec(options.getOutlierTableSpec())
                .setMethod(BigQueryIO.Write.Method.STREAMING_INSERTS)
                .build());
    return p.run();
  }
}
