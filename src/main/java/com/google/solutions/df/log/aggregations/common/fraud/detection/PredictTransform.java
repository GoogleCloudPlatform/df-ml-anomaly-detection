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

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.ByteArrayContent;
import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpContent;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.UriTemplate;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.discovery.Discovery;
import com.google.api.services.discovery.model.JsonSchema;
import com.google.api.services.discovery.model.RestDescription;
import com.google.api.services.discovery.model.RestMethod;
import com.google.auto.value.AutoValue;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.ToJson;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@AutoValue
public abstract class PredictTransform extends PTransform<PCollection<Row>, PCollection<Row>> {
  public static final Logger LOG = LoggerFactory.getLogger(PredictTransform.class);
  public static final List<String> scope =
      Arrays.asList("https://www.googleapis.com/auth/cloud-platform");
  public static Duration WINDOW_INTERVAL = Duration.standardSeconds(5);

  public abstract Integer batchSize();

  public abstract String projectId();

  public abstract Integer randomKey();

  public abstract String modelId();

  public abstract String versionId();

  public abstract Double probability();

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setBatchSize(Integer batchSize);

    public abstract Builder setProjectId(String projectId);

    public abstract Builder setRandomKey(Integer randomKey);

    public abstract Builder setModelId(String modelId);

    public abstract Builder setVersionId(String versionId);

    public abstract Builder setProbability(Double probability);

    public abstract PredictTransform build();
  }

  public static Builder newBuilder() {
    return new AutoValue_PredictTransform.Builder();
  }

  @Override
  public PCollection<Row> expand(PCollection<Row> input) {

    PCollection<Row> predictInput =
        input
            .apply(
                "Fixed Window",
                Window.<Row>into(FixedWindows.of(Duration.standardSeconds(5)))
                    .triggering(AfterWatermark.pastEndOfWindow())
                    .discardingFiredPanes()
                    .withAllowedLateness(Duration.ZERO))
            .apply(
                "ModifiedRow",
                ParDo.of(
                    new DoFn<Row, Row>() {
                      @ProcessElement
                      public void processElement(ProcessContext c) {
                        Row currentRow = c.element();
                        Row modifiedRow =
                            Row.withSchema(Util.prerdictonInputSchema)
                                .addValues(
                                    currentRow.getValue("step"),
                                    currentRow.getValue("type"),
                                    currentRow.getValue("amount"),
                                    currentRow.getValue("oldbalanceOrg"),
                                    currentRow.getValue("newbalanceOrig"),
                                    currentRow.getValue("oldbalanceDest"),
                                    currentRow.getValue("newbalanceDest"),
                                    currentRow.getValue("transactionId"))
                                .build();

                        LOG.debug("Modified Row {}", modifiedRow);
                        c.output(modifiedRow);
                      }
                    }))
            .setRowSchema(Util.prerdictonInputSchema);

    return predictInput
        .apply("RowToJson", ToJson.of())
        .apply("AddKey", WithKeys.of(new Random().nextInt(randomKey())))
        .apply("Batch", ParDo.of(new BatchRequest(batchSize())))
        .apply(
            "Predict",
            ParDo.of(new PredictRemote(projectId(), modelId(), versionId(), probability())))
        .setRowSchema(Util.prerdictonOutputSchema);
  }

  public static class BatchRequest extends DoFn<KV<Integer, String>, String> {
    private Integer batchSize;

    public BatchRequest(Integer batchSize) {
      this.batchSize = batchSize;
    }

    @StateId("elementsBag")
    private final StateSpec<BagState<String>> elementsBag = StateSpecs.bag();

    @TimerId("eventTimer")
    private final TimerSpec eventTimer = TimerSpecs.timer(TimeDomain.EVENT_TIME);

    @ProcessElement
    public void process(
        @Element KV<Integer, String> element,
        @StateId("elementsBag") BagState<String> elementsBag,
        @TimerId("eventTimer") Timer eventTimer,
        BoundedWindow w) {
      LOG.debug("Key {}", element.getKey());
      elementsBag.add(element.getValue());
      eventTimer.set(w.maxTimestamp());
    }

    @OnTimer("eventTimer")
    public void onTimer(
        @StateId("elementsBag") BagState<String> elementsBag, OutputReceiver<String> output) {
      AtomicInteger bufferSize = new AtomicInteger();
      List<String> rows = new ArrayList<>();
      elementsBag
          .read()
          .forEach(
              element -> {
                Integer elementSize = element.getBytes().length;
                boolean clearBuffer = (bufferSize.intValue() + elementSize.intValue() > batchSize);
                if (clearBuffer) {
                  LOG.debug("Clearing Rows {}", rows.size());
                  if (rows.size() > 0) output.output(emitResult(rows, rows.size()));
                  rows.clear();
                  bufferSize.set(0);
                  rows.add(element);
                  bufferSize.getAndAdd(Integer.valueOf(element.getBytes().length));

                } else {
                  rows.add(element);
                  bufferSize.getAndAdd(Integer.valueOf(element.getBytes().length));
                }
              });
      if (!rows.isEmpty() && rows.size() > 0) {
        LOG.debug("Remaning Rows {}", rows.size());
        output.output(emitResult(rows, rows.size()));
      }
    }
  }

  public static String emitResult(Iterable<String> records, Integer totalRows) {

    StringBuilder builder = new StringBuilder();
    builder.append("{\"signature_name\":\"predict\",\"instances\": [");
    builder.append("\n");
    builder.append(
        StreamSupport.stream(records.spliterator(), false).collect(Collectors.joining(",")));
    builder.append("\n]}");
    LOG.info("Builder Size {}, Rows {}", builder.toString().getBytes().length, totalRows);
    return builder.toString();
  }

  public static class PredictRemote extends DoFn<String, Row> {

    private String projectId;
    private String modelId;
    private String versionId;
    private Double probability;
    private GenericUrl url;
    private String contentType;
    private RestMethod method;
    private HttpTransport httpTransport;
    private GoogleCredential credential;
    private Gson json;
    private final Counter numberOfOutliersFound =
        Metrics.counter(ReadTransactionTransform.class, "numberOfOutliersFound");

    public PredictRemote(String projectId, String modelId, String versionId, Double probability) {
      this.projectId = projectId;
      this.modelId = modelId;
      this.versionId = versionId;
      this.probability = probability;
      this.contentType = "application/json";
    }

    @StartBundle
    public void startBundle() throws GeneralSecurityException, IOException {
      json = new Gson();
      httpTransport = GoogleNetHttpTransport.newTrustedTransport();
      JsonFactory jsonFactory = JacksonFactory.getDefaultInstance();
      Discovery discovery =
          new Discovery.Builder(httpTransport, jsonFactory, null)
              .setApplicationName("fin_serv_fraud_detection")
              .build();
      RestDescription api = discovery.apis().getRest("ml", "v1").execute();
      method = api.getResources().get("projects").getMethods().get("predict");
      JsonSchema param = new JsonSchema();
      param.set(
          "name",
          String.format("projects/%s/models/%s/versions/%s", projectId, modelId, versionId));
      url = new GenericUrl(UriTemplate.expand(api.getBaseUrl() + method.getPath(), param, true));
      credential = GoogleCredential.getApplicationDefault().createScoped(scope);
      LOG.debug("Url {}", url.toString());
    }

    @FinishBundle
    public void finishBundle() {}

    @ProcessElement
    public void processElement(ProcessContext c) throws IOException {

      HttpContent content = new ByteArrayContent(contentType, c.element().getBytes());
      HttpRequestFactory requestFactory = httpTransport.createRequestFactory(credential);
      HttpRequest request = requestFactory.buildRequest(method.getHttpMethod(), url, content);
      String response = request.execute().parseAsString();
      JsonArray convertedObject =
          json.fromJson(response, JsonObject.class).getAsJsonArray("predictions");
      LOG.debug("Response Size {} rows", convertedObject.size());

      convertedObject.forEach(
          element -> {
            JsonObject jo = element.getAsJsonObject();
            String transactionId = jo.get("transactionId").getAsJsonArray().get(0).getAsString();
            Double logistic = jo.get("logistic").getAsJsonArray().get(0).getAsDouble();
            if (logistic >= probability) {
              Row row =
                  Row.withSchema(Util.prerdictonOutputSchema)
                      .addValues(transactionId, logistic, element.toString())
                      .build();
              LOG.debug("Predict Output {}", row.toString());
              numberOfOutliersFound.inc();
              c.output(row);
            }
          });
    }
  }
}
