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
package com.google.solutions.df.log.aggregations.common;

import com.google.auto.value.AutoValue;
import com.google.cloud.dlp.v2.DlpServiceClient;
import com.google.privacy.dlp.v2.ContentItem;
import com.google.privacy.dlp.v2.DeidentifyContentRequest;
import com.google.privacy.dlp.v2.DeidentifyContentResponse;
import com.google.privacy.dlp.v2.FieldId;
import com.google.privacy.dlp.v2.ProjectName;
import com.google.privacy.dlp.v2.Table;
import com.google.privacy.dlp.v2.Value;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupIntoBatches;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@AutoValue
public abstract class DLPTransform extends PTransform<PCollection<Row>, PCollection<Row>> {
  public static final Logger LOG = LoggerFactory.getLogger(DLPTransform.class);

  @Nullable
  public abstract String inspectTemplateName();

  @Nullable
  public abstract String deidTemplateName();

  public abstract Integer batchSize();

  public abstract String projectId();

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setInspectTemplateName(String inspectTemplateName);

    public abstract Builder setDeidTemplateName(String deidTemplateName);

    public abstract Builder setBatchSize(Integer batchSize);

    public abstract Builder setProjectId(String projectId);

    public abstract DLPTransform build();
  }

  public static Builder newBuilder() {
    return new AutoValue_DLPTransform.Builder();
  }

  @Override
  public PCollection<Row> expand(PCollection<Row> input) {

    if (deidTemplateName() == null) {

      return input.apply(
          "Convert To BqRow",
          MapElements.via(new SimpleFunction<Row, Row>((Row bqRow) -> bqRow) {}));
    }
    return input
        .apply("Convert To DLP Row", ParDo.of(new ConvertToDLPRow()))
        .apply("With Keys", WithKeys.of(UUID.randomUUID().toString()))
        .apply("Group Into Batches", GroupIntoBatches.<String, Table.Row>ofSize(batchSize()))
        .setCoder(
            KvCoder.of(StringUtf8Coder.of(), IterableCoder.of(ProtoCoder.of(Table.Row.class))))
        .apply(
            "DLP Tokenization",
            ParDo.of(
                new DLPTokenizationDoFn(projectId(), deidTemplateName(), inspectTemplateName())))
        .apply("ConvertToBQRow", MapElements.via(new ConvertToBQRow()));
  }

  public static class DLPTokenizationDoFn extends DoFn<KV<String, Iterable<Table.Row>>, Table.Row> {
    private DlpServiceClient dlpServiceClient;
    private boolean inspectTemplateExist;
    private String dlpProjectId;
    private String deIdentifyTemplateName;
    private String inspectTemplateName;
    private DeidentifyContentRequest.Builder requestBuilder;

    public DLPTokenizationDoFn(
        String dlpProjectId, String deIdentifyTemplateName, String inspectTemplateName) {
      this.dlpProjectId = dlpProjectId;
      this.dlpServiceClient = null;
      this.deIdentifyTemplateName = deIdentifyTemplateName;
      this.inspectTemplateName = inspectTemplateName;
      this.inspectTemplateExist = false;
    }

    @Setup
    public void setup() {
      if (this.inspectTemplateName != null) {
        this.inspectTemplateExist = true;
      }
      if (this.deIdentifyTemplateName != null) {
        this.requestBuilder =
            DeidentifyContentRequest.newBuilder()
                .setParent(ProjectName.of(this.dlpProjectId).toString())
                .setDeidentifyTemplateName(this.deIdentifyTemplateName);
        if (this.inspectTemplateExist) {
          this.requestBuilder.setInspectTemplateName(this.inspectTemplateName);
        }
      }
    }

    @StartBundle
    public void startBundle() throws SQLException {

      try {
        this.dlpServiceClient = DlpServiceClient.create();

      } catch (IOException e) {
        LOG.error("Failed to create DLP Service Client", e.getMessage());
        throw new RuntimeException(e);
      }
    }

    @FinishBundle
    public void finishBundle() throws Exception {
      if (this.dlpServiceClient != null) {
        this.dlpServiceClient.close();
      }
    }

    @ProcessElement
    public void processElement(ProcessContext c) {

      List<FieldId> dlpTableHeaders =
          Util.bqLogSchema.getFieldNames().stream()
              .map(header -> FieldId.newBuilder().setName(header).build())
              .collect(Collectors.toList());

      List<Table.Row> rows = new ArrayList<>();
      c.element().getValue().forEach(rows::add);
      Table dlpTable = Table.newBuilder().addAllHeaders(dlpTableHeaders).addAllRows(rows).build();
      ContentItem tableItem = ContentItem.newBuilder().setTable(dlpTable).build();
      this.requestBuilder.setItem(tableItem);
      DeidentifyContentResponse response =
          dlpServiceClient.deidentifyContent(this.requestBuilder.build());
      Table tokenizedData = response.getItem().getTable();
      List<Table.Row> outputRows = tokenizedData.getRowsList();
      outputRows.forEach(
          row -> {
            LOG.debug("Tokenized Row {}", row);
            c.output(row);
          });
    }
  }

  public static class ConvertToBQRow extends SimpleFunction<Table.Row, Row> {
    @Override
    public Row apply(Table.Row input) {

      Row bqRow =
          Row.withSchema(Util.bqLogSchema)
              .addValue(input.getValues(0).getStringValue())
              .addValue(input.getValues(1).getStringValue())
              .addValue(input.getValues(2).getStringValue())
              .addValue(Integer.valueOf(input.getValues(3).getStringValue()))
              .addValue(Integer.valueOf(input.getValues(4).getStringValue()))
              .addValue(Integer.valueOf(input.getValues(5).getStringValue()))
              .addValue(Integer.valueOf(input.getValues(6).getStringValue()))
              .addValue(Integer.valueOf(input.getValues(7).getStringValue()))
              .addValue(Double.valueOf(input.getValues(8).getStringValue()))
              .addValue(Integer.valueOf(input.getValues(9).getStringValue()))
              .addValue(Integer.valueOf(input.getValues(10).getStringValue()))
              .addValue(Double.valueOf(input.getValues(11).getStringValue()))
              .addValue(Integer.valueOf(input.getValues(12).getStringValue()))
              .addValue(Integer.valueOf(input.getValues(13).getStringValue()))
              .addValue(Double.valueOf(input.getValues(14).getStringValue()))
              .build();

      LOG.info("BQ Row {}", bqRow.toString());
      return bqRow;
    }
  }

  public static class ConvertToDLPRow extends DoFn<Row, Table.Row> {
    @ProcessElement
    public void processElement(ProcessContext c) {

      Iterator<Object> row = c.element().getValues().iterator();
      Table.Row.Builder tableRowBuilder = Table.Row.newBuilder();

      while (row.hasNext()) {

        tableRowBuilder.addValues(Value.newBuilder().setStringValue(row.next().toString()));
      }
      Table.Row dlpRow = tableRowBuilder.build();
      LOG.info("DLPRow {}", dlpRow.toString());
      c.output(dlpRow);
    }
  }
}
