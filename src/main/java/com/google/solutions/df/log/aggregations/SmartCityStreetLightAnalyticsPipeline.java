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
import com.google.cloud.vision.v1.AnnotateImageRequest;
import com.google.cloud.vision.v1.BatchAnnotateImagesResponse;
import com.google.cloud.vision.v1.Feature;
import com.google.cloud.vision.v1.Image;
import com.google.cloud.vision.v1.ImageAnnotatorClient;
import com.google.cloud.vision.v1.ImageSource;
import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.solutions.df.log.aggregations.common.Util;
import com.google.solutions.df.log.aggregations.common.Util.ImageCategory;
import java.io.IOException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.extensions.gcp.util.gcsfs.GcsPath;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.transforms.Filter;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.JsonToRow;
import org.apache.beam.sdk.transforms.JsonToRow.ParseResult;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
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
  static final String BUCKET_NAME = "smart-city-mqtt-data-generator";

  static final List<Feature> features =
      Collections.singletonList(Feature.newBuilder().setType(Feature.Type.LABEL_DETECTION).build());

  static DateTimeFormatter bqTimestamp = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");

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
                            dateTime.withZoneSameInstant(ZoneId.of("UTC")).format(bqTimestamp);
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
    PCollection<Row> validatedSensor =
        sensorRow
            .apply(
                "Co2Check",
                ParDo.of(
                    new DoFn<Row, Row>() {
                      @ProcessElement
                      public void processElement(ProcessContext c) {
                        Row sensorData = c.element();
                        String objectName;
                        if (sensorData.getDouble("co2") >= CO2_THRESHOLD) {
                          ImageCategory category =
                              Integer.parseInt(sensorData.getString("deviceId")) % 2 == 0
                                  ? ImageCategory.fire
                                  : ImageCategory.smoke;
                          objectName =
                              String.format("%s/%s", "images", Util.getRandomImageName(category));

                        } else {
                          objectName =
                              String.format(
                                  "%s/%s",
                                  "images", Util.getRandomImageName(ImageCategory.regular));
                        }

                        // Row modifiedRow =
                        //      Row.fromRow(sensorData)
                        //          .withFieldValue("imageUrl", url.getFileName().toString())
                        //          .build();
                        Row modifiedRow =
                            Row.withSchema(Util.sensorDataBQSchema)
                                .addValues(
                                    Util.getTimeStamp(),
                                    sensorData.getDouble("co2"),
                                    sensorData.getDouble("temp"),
                                    sensorData.getDouble("humid"),
                                    sensorData.getDouble("pir"),
                                    sensorData.getDouble("bright"),
                                    sensorData.getString("deviceId"),
                                    sensorData.getString("city"),
                                    GcsPath.fromComponents(BUCKET_NAME, objectName)
                                        .toUri()
                                        .toString(),
                                    sensorData.getInt32("signalState"))
                                .build();
                        LOG.debug(modifiedRow.toString());
                        c.output(modifiedRow);
                      }
                    }))
            .setRowSchema(Util.sensorDataBQSchema);

    PCollection<Row> annotationRow =
        validatedSensor
            .apply(
                "FilterCo2Threshold",
                Filter.<Row>create().whereFieldName("co2", c -> (double) c >= CO2_THRESHOLD))
            .setRowSchema(Util.sensorDataBQSchema)
            .apply(
                "LabelAnnotation",
                ParDo.of(
                    new DoFn<Row, Row>() {
                      private transient ImageAnnotatorClient imageAnnotatorClient;

                      @Setup
                      public void setup() throws IOException {
                        imageAnnotatorClient = ImageAnnotatorClient.create();
                      }

                      @Teardown
                      public void teardown() {
                        imageAnnotatorClient.close();
                      }
                      @ProcessElement
                      public void processElement(ProcessContext c) {
                        List<AnnotateImageRequest> requestList = new ArrayList<>();
                        Row row = c.element();
                        String imageUri = row.getString("imageUrl");
                        String deviceId = row.getString("deviceId");
                        ImageSource imageSource =
                            ImageSource.newBuilder().setGcsImageUri(imageUri).build();
                        Image image = Image.newBuilder().setSource(imageSource).build();
                        AnnotateImageRequest request =
                            AnnotateImageRequest.newBuilder()
                                .setImage(image)
                                .addAllFeatures(features)
                                .build();
                        requestList.add(request);
                        BatchAnnotateImagesResponse batchAnnotateImagesResponse =
                            imageAnnotatorClient.batchAnnotateImages(requestList);
                        batchAnnotateImagesResponse
                            .getResponsesList()
                            .forEach(
                                response -> {
                                  response
                                      .getLabelAnnotationsList()
                                      .forEach(
                                          annotation -> {
                                            Row annotationResponse = Row.withSchema(Util.annotationDataBQSchema)
                                                .addValues(
                                                    Util.getTimeStamp(),
                                                    image.getSource().getGcsImageUri(),
                                                    deviceId,
                                                    annotation.getMid(),
                                                    annotation.getDescription(),
                                                    annotation.getScore(),
                                                    annotation.getTopicality())
                                                .build();
                                            LOG.info(annotationResponse.toString());
                                            c.output(annotationResponse);
                                          });
                                });
                      }
                    })).setRowSchema(Util.annotationDataBQSchema);

    validatedSensor.apply(
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

    annotationRow.apply(
        "WriteAnnotationData",
        BigQueryIO.<Row>write()
            .to(options.getAnnotationTableSpec())
            .useBeamSchema()
            .withTimePartitioning(timePartitioning)
            .withClustering(clustering)
            .ignoreInsertIds()
            .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
            .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER));
    return p.run();
  }
}
