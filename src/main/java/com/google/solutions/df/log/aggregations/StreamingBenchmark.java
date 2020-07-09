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

import com.github.vincentrussell.json.datagenerator.JsonDataGenerator;
import com.github.vincentrussell.json.datagenerator.JsonDataGeneratorException;
import com.github.vincentrussell.json.datagenerator.impl.JsonDataGeneratorImpl;
import com.google.common.collect.Maps;
import com.google.common.io.ByteStreams;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.fs.MatchResult.Metadata;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamingBenchmark {

  public static final String MESSAGE_TYPE_HEADER = "event_type";
  public static final Logger LOG = LoggerFactory.getLogger(MessageGeneratorFn.class);

  public interface Options extends PipelineOptions {
    @Description("The QPS which the benchmark should output to Pub/Sub.")
    @Required
    Long getQps();

    void setQps(Long value);

    @Description("The path to the schema to generate.")
    @Required
    String getSchemaLocation();

    void setSchemaLocation(String value);

    @Description("publish topic")
    @Required
    String getTopic();

    void setTopic(String value);

    @Description("Event type attribute")
    @Required
    String getEventType();

    void setEventType(String value);
  }

  public static void main(String[] args) {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

    run(options);
  }

  public static PipelineResult run(Options options) {

    // Create the pipeline
    Pipeline pipeline = Pipeline.create(options);

    /*
     * Steps: 1) Trigger at the supplied QPS 2) Generate messages containing fake
     * data 3) Write messages to Pub/Sub
     */
    pipeline
        .apply(
            "Trigger",
            GenerateSequence.from(0L).withRate(options.getQps(), Duration.standardSeconds(1L)))
        .apply(
            "GenerateMessages",
            ParDo.of(new MessageGeneratorFn(options.getSchemaLocation(), options.getEventType())))
        .apply("WriteToPubsub", PubsubIO.writeMessages().to(options.getTopic()));

    return pipeline.run();
  }

  static class MessageGeneratorFn extends DoFn<Long, PubsubMessage> {

    private final String schemaLocation;
    private String schema;
    private String eventType;

    private transient JsonDataGenerator dataGenerator;

    MessageGeneratorFn(String schemaLocation, String eventType) {
      this.schemaLocation = schemaLocation;
      this.eventType = eventType;
    }

    @Setup
    public void setup() throws IOException {
      dataGenerator = new JsonDataGeneratorImpl();
      Metadata metadata = FileSystems.matchSingleFileSpec(schemaLocation);

      // Copy the schema file into a string which can be used for generation.
      try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream()) {
        try (ReadableByteChannel readerChannel = FileSystems.open(metadata.resourceId())) {
          try (WritableByteChannel writerChannel = Channels.newChannel(byteArrayOutputStream)) {
            ByteStreams.copy(readerChannel, writerChannel);
          }
        }

        schema = byteArrayOutputStream.toString();
      }
    }

    @ProcessElement
    public void processElement(ProcessContext context)
        throws IOException, JsonDataGeneratorException {

      byte[] payload;
      Map<String, String> attributes = Maps.newHashMap();
      attributes.put(MESSAGE_TYPE_HEADER, this.eventType);

      // Generate the fake JSON according to the schema.
      try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream()) {
        dataGenerator.generateTestDataJson(schema, byteArrayOutputStream);

        payload = byteArrayOutputStream.toByteArray();
      }
      PubsubMessage message = new PubsubMessage(payload, attributes);
      LOG.info(message.getPayload().toString());
      context.output(message);
    }
  }
}
