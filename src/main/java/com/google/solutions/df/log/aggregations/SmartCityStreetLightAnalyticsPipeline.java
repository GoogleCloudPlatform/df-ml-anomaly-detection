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

import com.google.api.services.bigquery.model.Clustering;
import com.google.api.services.bigquery.model.TimePartitioning;
import com.google.cloud.vision.v1.AnnotateImageResponse;
import com.google.cloud.vision.v1.Feature;
import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.solutions.df.log.aggregations.common.Util;
import com.google.solutions.df.log.aggregations.common.Util.ImageCategory;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.extensions.gcp.util.gcsfs.GcsPath;
import org.apache.beam.sdk.extensions.ml.CloudVision;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.JsonToRow;
import org.apache.beam.sdk.transforms.JsonToRow.ParseResult;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SmartCityStreetLightAnalyticsPipeline {
  public static final Logger LOG =
      LoggerFactory.getLogger(SmartCityStreetLightAnalyticsPipeline.class);
  static final TimePartitioning timePartitioning =
      new TimePartitioning().setType("HOUR").setField("timeStamp");
  static final Clustering clustering = new Clustering().setFields(ImmutableList.of("timeStamp"));

  static final TimePartitioning timeStatePartitioning =
      new TimePartitioning().setType("HOUR").setField("timeStamp");
  static final Clustering clusteringState =
      new Clustering().setFields(ImmutableList.of("connectState"));
  static final Double CO2_THRESHOLD = 500.00;
  static final TupleTag<Row> validatedSensorData = new TupleTag<Row>() {};
  static final TupleTag<Row> validatedCo2Data = new TupleTag<Row>() {};
  static final List<Feature> features =
      Collections.singletonList(Feature.newBuilder().setType(Feature.Type.LABEL_DETECTION).build());

  static DateTimeFormatter bqTimstamp = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");

  public static void main(String args[]) {

    SmartCityStreetLightAnalyticsPipelineOptions options =
        PipelineOptionsFactory.fromArgs(args)
            .withValidation()
            .as(SmartCityStreetLightAnalyticsPipelineOptions.class);

    run(options);
  }

  public static PipelineResult run(SmartCityStreetLightAnalyticsPipelineOptions options) {
    Pipeline p = Pipeline.create(options);
    // read sensor data
    PCollection<String> sensorData =
        p.apply(
            "ReadSensorData", PubsubIO.readStrings().fromSubscription(options.getSensorDataSub()));

    // read state data
    PCollection<String> stateData =
        p.apply(
            "ReadStateData", PubsubIO.readStrings().fromSubscription(options.getStateDataSub()));

    ParseResult result =
        sensorData.apply("ConvertToRow", JsonToRow.withExceptionReporting(Util.sensorDataBQSchema));
    PCollection<Row> sensorRow = result.getResults().setRowSchema(Util.sensorDataBQSchema);

    PCollection<Row> stateRow =
        stateData
            .apply(
                "ProcessStateData",
                ParDo.of(
                    new DoFn<String, Row>() {
                      Gson gson;

                      @Setup
                      public void setup() {
                        gson = new Gson();
                      }

                      @ProcessElement
                      public void processElement(ProcessContext c) {
                        String json = c.element();
                        JsonObject data = gson.fromJson(json, JsonObject.class);
                        ZonedDateTime dateTime =
                            ZonedDateTime.parse(data.get("timestamp").getAsString());
                        String timeStamp =
                            dateTime.withZoneSameInstant(ZoneId.of("UTC")).format(bqTimstamp);
                        Row row =
                            Row.withSchema(Util.stateDataBQSchema)
                                .addValues(
                                    timeStamp,
                                    data.getAsJsonObject("labels").get("device_id").toString(),
                                    data.getAsJsonObject("jsonPayload").get("eventType").toString())
                                .build();
                        LOG.info(row.toString());
                        c.output(row);
                      }
                    }))
            .setRowSchema(Util.stateDataBQSchema);

    // // update with Image Url based on co2 value
    PCollectionTuple validatedSensor =
        sensorRow.apply(
            "Co2Check",
            ParDo.of(
                    new DoFn<Row, Row>() {
                      @ProcessElement
                      public void processElement(ProcessContext c, MultiOutputReceiver out) {
                        Row sensorData = c.element();
                        String bucketName = "gs://smart-city-mqtt-data-generator/images/";
                        if (sensorData.getDouble("co2") >= CO2_THRESHOLD) {
                          ImageCategory category =
                              Integer.parseInt(sensorData.getString("deviceId")) % 2 == 0
                                  ? ImageCategory.fire
                                  : ImageCategory.smoke;
                          GcsPath url =
                              GcsPath.fromUri(
                                  String.format(
                                      "%s%s", bucketName, Util.getRandomImageName(category)));
                          LOG.info(url.toString());
                          Row modifiedRow =
                              Row.fromRow(sensorData)
                                  .withFieldValue("imageUrl", url.getFileName().toString())
                                  .build();
                          out.get(validatedCo2Data).output(modifiedRow);
                        } else {
                          GcsPath url =
                              GcsPath.fromUri(
                                  String.format(
                                      "%s%s",
                                      bucketName, Util.getRandomImageName(ImageCategory.regular)));
                          LOG.info(url.toString());
                          Row modifiedRow =
                              Row.fromRow(sensorData)
                                  .withFieldValue("imageUrl", url.getFileName().toString())
                                  .build();

                          out.get(validatedSensorData).output(modifiedRow);
                        }
                      }
                    })
                .withOutputTags(validatedSensorData, TupleTagList.of(validatedCo2Data)));

    PCollection<Row> validatedSensorRow =
        validatedSensor.get(validatedSensorData).setRowSchema(Util.sensorDataBQSchema);
    PCollection<Row> validatedCo2Row =
        validatedSensor.get(validatedCo2Data).setRowSchema(Util.sensorDataBQSchema);

    PCollection<String> imageUrl =
        validatedCo2Row.apply(
            MapElements.into(TypeDescriptors.strings()).via((Row r) -> r.getString("imageUrl")));
    PCollection<List<AnnotateImageResponse>> annotationResponses =
        imageUrl.apply(CloudVision.annotateImagesFromGcsUri(null, features, 1, 1));
    annotationResponses.apply(
        "ProcessLObjectAnnotation",
        ParDo.of(
            new DoFn<List<AnnotateImageResponse>, Row>() {
              @ProcessElement
              public void processElement(@Element List<AnnotateImageResponse> e) {
                e.forEach(
                    annotation -> {
                      LOG.info(annotation.toString());
                    });
              }
            }));

    validatedSensorRow.apply(
        "WriteSensorData",
        BigQueryIO.<Row>write()
            .to(options.getSensorTableSpec())
            .useBeamSchema()
            .ignoreInsertIds()
            .withTimePartitioning(timePartitioning)
            .withClustering(clustering)
            .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
            .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER));

    stateRow.apply(
        "WriteStateData",
        BigQueryIO.<Row>write()
            .to(options.getStateTableSpec())
            .useBeamSchema()
            .withTimePartitioning(timeStatePartitioning)
            .withClustering(clusteringState)
            .ignoreInsertIds()
            .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
            .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER));

    return p.run();
  }
}
