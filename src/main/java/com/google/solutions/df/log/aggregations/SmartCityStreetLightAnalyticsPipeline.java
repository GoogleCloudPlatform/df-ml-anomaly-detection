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
import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.solutions.df.log.aggregations.common.Util;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.JsonToRow;
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

    PCollection<Row> sensorRow =
        sensorData.apply("ConvertToRow", JsonToRow.withSchema(Util.sensorDataBQSchema));

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
                        // DC -0 C-1
                        Row row =
                            Row.withSchema(Util.stateDataBQSchema)
                                .addValues(
                                    data.get("timestamp").toString(),
                                    data.getAsJsonObject("labels").get("device_id").toString(),
                                    data.getAsJsonObject("jsonPayload")
                                            .get("eventType")
                                            .equals("DISCONNECT")
                                        ? 0
                                        : 1)
                                .build();
                        LOG.info(row.toString());
                        c.output(row);
                      }
                    }))
            .setRowSchema(Util.stateDataBQSchema);

    sensorRow.apply(
        "WriteSensorData",
        BigQueryIO.<Row>write()
            .to(options.getSensorTableSpec())
            .useBeamSchema()
            .withTimePartitioning(timePartitioning)
            .withClustering(clustering)
            .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
            .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER));

    stateRow.apply(
        "WriteStateData",
        BigQueryIO.<Row>write()
            .to(options.getStateTableSpec())
            .useBeamSchema()
            .ignoreInsertIds()
            .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
            .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER));

    return p.run();
  }
}
