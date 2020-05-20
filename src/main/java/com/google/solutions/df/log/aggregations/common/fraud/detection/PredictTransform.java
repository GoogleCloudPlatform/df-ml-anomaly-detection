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
import org.apache.beam.sdk.schemas.transforms.DropFields;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.JsonToRow;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.ToJson;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
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

  public abstract Integer batchSize();

  public abstract String projectId();

  public abstract Integer randomKey();

  public abstract String modelId();

  public abstract String versionId();

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setBatchSize(Integer batchSize);

    public abstract Builder setProjectId(String projectId);

    public abstract Builder setRandomKey(Integer randomKey);

    public abstract Builder setModelId(String modelId);

    public abstract Builder setVersionId(String versionId);

    public abstract PredictTransform build();
  }

  public static Builder newBuilder() {
    return new AutoValue_PredictTransform.Builder();
  }

  @Override
  public PCollection<Row> expand(PCollection<Row> input) {

    return input
        .apply(
            "ModifySchema", DropFields.fields("nameOrig", "nameDest", "isFlaggedFraud", "isFraud"))
        .setRowSchema(Util.prerdictonInputSchema)
        .apply("RowToJson", ToJson.of())
        .apply("AddKey", WithKeys.of(new Random().nextInt(randomKey())))
        .apply("Batch", ParDo.of(new BatchRequest(batchSize())))
        .apply("Predict", ParDo.of(new PredictRemote(projectId(), modelId(), versionId())))
        .apply("JsontoRow", JsonToRow.withSchema(Util.prerdictonOutputSchema))
        .setRowSchema(Util.prerdictonOutputSchema)
        .apply(
            "Print",
            ParDo.of(
                new DoFn<Row, Row>() {
                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    LOG.info("Row BQ {}", c.element());
                    c.output(c.element());
                  }
                }));
  }

  public static class BatchRequest extends DoFn<KV<Integer, String>, String> {
    private Integer batchSize;

    public BatchRequest(Integer batchSize) {
      this.batchSize = batchSize;
    }

    @StateId("elementsBag")
    private final StateSpec<BagState<String>> elementsBag = StateSpecs.bag();

    @TimerId("eventTimer")
    private final TimerSpec eventTimer = TimerSpecs.timer(TimeDomain.PROCESSING_TIME);

    @ProcessElement
    public void process(
        @Element KV<Integer, String> element,
        @StateId("elementsBag") BagState<String> elementsBag,
        @TimerId("eventTimer") Timer eventTimer,
        BoundedWindow w) {
      elementsBag.add(element.getValue());
      eventTimer.offset(Duration.standardSeconds(2)).setRelative();
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
                  output.output(emitResult(rows));
                  rows.clear();
                  bufferSize.set(0);
                  rows.add(element);
                  bufferSize.getAndAdd(Integer.valueOf(element.getBytes().length));

                } else {
                  rows.add(element);
                  bufferSize.getAndAdd(Integer.valueOf(element.getBytes().length));
                }
              });
      if (!rows.isEmpty()) {
        LOG.debug("Remaning Rows {}", rows.size());
        output.output(emitResult(rows));
      }
    }
  }

  public static String emitResult(Iterable<String> records) {

    StringBuilder builder = new StringBuilder();
    builder.append("{\"signature_name\":\"predict\",\"instances\": [");
    builder.append("\n");
    builder.append(
        StreamSupport.stream(records.spliterator(), false).collect(Collectors.joining(", ")));
    builder.append("\n]}");
    return builder.toString();
  }

  public static class PredictRemote extends DoFn<String, String> {

    private String projectId;
    private String modelId;
    private String versionId;
    private GenericUrl url;
    private String contentType;
    private RestMethod method;
    private HttpTransport httpTransport;

    public PredictRemote(String projectId, String modelId, String versionId) {
      this.projectId = projectId;
      this.modelId = modelId;
      this.versionId = versionId;
      this.contentType = "application/json";
    }

    @Setup
    public void setup() throws GeneralSecurityException, IOException {
      httpTransport = GoogleNetHttpTransport.newTrustedTransport();
      JsonFactory jsonFactory = JacksonFactory.getDefaultInstance();
      Discovery discovery = new Discovery.Builder(httpTransport, jsonFactory, null).build();
      RestDescription api = discovery.apis().getRest("ml", "v1").execute();
      method = api.getResources().get("projects").getMethods().get("predict");
      JsonSchema param = new JsonSchema();
      param.set(
          "name",
          String.format("projects/%s/models/%s/versions/%s", projectId, modelId, versionId));
      url = new GenericUrl(UriTemplate.expand(api.getBaseUrl() + method.getPath(), param, true));
      LOG.info("Url {}", url.toString());
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws IOException {

      HttpContent content = new ByteArrayContent(contentType, c.element().getBytes());

      GoogleCredential credential = GoogleCredential.getApplicationDefault().createScoped(scope);
      HttpRequestFactory requestFactory = httpTransport.createRequestFactory(credential);
      HttpRequest request = requestFactory.buildRequest(method.getHttpMethod(), url, content);
      String response = request.execute().parseAsString();
      JsonObject convertedObject = new Gson().fromJson(response, JsonObject.class);
      convertedObject
          .getAsJsonArray("predictions")
          .forEach(
              element -> {
                LOG.debug("row {}", element.toString());
                c.output(element.toString());
                element.getAsJsonObject().get("transactionId").toString();
                element.getAsJsonObject().get("logistic");
                Row.withSchema(Util.prerdictonOutputSchema)
                    .addValues(
                        element.getAsJsonObject().get("transactionId").toString(),
                        element.getAsJsonObject().get("logistic"),
                        convertedObject.toString())
                    .build();
              });
    }
  }
}
