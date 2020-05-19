package com.google.solutions.df.log.aggregations.common.fraud.detection;

import com.google.api.client.googleapis.auth.oauth2.*;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.ByteArrayContent;
import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpContent;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.UriTemplate;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.discovery.Discovery;
import com.google.api.services.discovery.model.JsonSchema;
import com.google.api.services.discovery.model.RestDescription;
import com.google.api.services.discovery.model.RestMethod;
import com.google.auto.value.AutoValue;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
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
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@AutoValue
public abstract class PredictTransform extends PTransform<PCollection<String>, PCollection<Row>> {
  public static final Logger LOG = LoggerFactory.getLogger(PredictTransform.class);
  private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();

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
  public PCollection<Row> expand(PCollection<String> input) {

    return input
        .apply("Add Key", WithKeys.of(new Random().nextInt(randomKey())))
        .apply("Batch", ParDo.of(new BatchRequest(batchSize())))
        .apply("Predict", ParDo.of(new PredictRemote(projectId(), modelId(), versionId())));
  }

  public static class BatchRequest extends DoFn<KV<Integer, String>, Iterable<String>> {
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
        @StateId("elementsBag") BagState<String> elementsBag,
        OutputReceiver<Iterable<String>> output) {
      AtomicInteger bufferSize = new AtomicInteger();
      List<String> rows = new ArrayList<>();
      elementsBag
          .read()
          .forEach(
              element -> {
                Integer elementSize = element.getBytes().length;
                boolean clearBuffer = (bufferSize.intValue() + elementSize.intValue() > batchSize);
                if (clearBuffer) {
                  output.output(rows);
                  LOG.info("Clearing Rows {}", rows.size());
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
        LOG.info("Remaning Rows {}", rows.size());
        output.output(rows);
      }
    }
  }

  public static class PredictRemote extends DoFn<Iterable<String>, Row> {

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

      StringBuilder builder = new StringBuilder();
      builder.append("{\"instances\": [");
      builder.append("\n");
      
      
      c.element()
          .forEach(
              row -> {
                builder.append(row);
                builder.append(",");
                builder.append("\n");
              });
      builder.append("]}");
      LOG.info("request {}",builder.toString());
      HttpContent content = new ByteArrayContent("application/json", builder.toString().getBytes());
      List<String> scopes = new ArrayList<>();
      scopes.add("https://www.googleapis.com/auth/cloud-platform");

      GoogleCredential credential = GoogleCredential.getApplicationDefault().createScoped(scopes);
      // GoogleCredential credential = GoogleCredential.getApplicationDefault();
      HttpRequestFactory requestFactory = httpTransport.createRequestFactory(credential);
      HttpRequest request = requestFactory.buildRequest(method.getHttpMethod(), url, content);

      HttpResponse response = request.execute();
      BufferedReader reader =
          new BufferedReader(new InputStreamReader(response.getContent(), "utf-8"), 8);
      String line = null;
      while ((line = reader.readLine()) != null) {
        LOG.info("Line {}", line);
      }
    }
  }
}
