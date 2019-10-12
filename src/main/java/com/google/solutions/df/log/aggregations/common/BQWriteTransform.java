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

import com.google.api.services.bigquery.model.TimePartitioning;
import com.google.auto.value.AutoValue;
import javax.annotation.Nullable;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@AutoValue
public abstract class BQWriteTransform extends PTransform<PCollection<KV<Row, Row>>, WriteResult> {

  private static final Logger LOG = LoggerFactory.getLogger(BQWriteTransform.class);
  private static final Integer NUM_OF_SHARDS = 10;

  @Nullable
  public abstract Integer batchFrequency();

  public abstract BigQueryIO.Write.Method method();

  public abstract String tableSpec();

  public static Builder newBuilder() {
    return new AutoValue_BQWriteTransform.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setBatchFrequency(Integer batchFrequency);

    public abstract Builder setTableSpec(String tableSpec);

    public abstract Builder setMethod(BigQueryIO.Write.Method method);

    public abstract BQWriteTransform build();
  }

  @Override
  public WriteResult expand(PCollection<KV<Row, Row>> row) {

    switch (method()) {
      case FILE_LOADS:
        return row.apply("Merge Rows", ParDo.of(new MergeDoFn()))
            .apply(
                BigQueryIO.<Row>write()
                    .to(tableSpec())
                    .useBeamSchema()
                    .withMethod(BigQueryIO.Write.Method.FILE_LOADS)
                    .withTriggeringFrequency(Duration.standardMinutes(batchFrequency()))
                    .withNumFileShards(NUM_OF_SHARDS)
                    .withTimePartitioning(new TimePartitioning().setType("DAY")));

      default:
        return row.apply("Merge Rows", ParDo.of(new MergeDoFn()))
            .setRowSchema(Util.bqLogSchema)
            .apply(
                BigQueryIO.<Row>write()
                    .to(tableSpec())
                    .useBeamSchema()
                    .withTimePartitioning(new TimePartitioning().setType("DAY")));
    }
  }

  public class MergeDoFn extends DoFn<KV<Row, Row>, Row> {
    @ProcessElement
    public void processsElement(ProcessContext c) {
      c.output(
          Row.withSchema(Util.bqLogSchema)
              .addValues(
                  c.element().getKey().getString("subscriberId"),
                  c.element().getKey().getString("dstSubnet"),
                  Util.getTimeStamp(),
                  c.element().getValue().getInt32("number_of_records"),
                  c.element().getValue().getInt32("number_of_ips"),
                  c.element().getValue().getInt32("number_of_ports"),
                  c.element().getValue().getInt32("max_tx_bytes"),
                  c.element().getValue().getInt32("min_tx_bytes"),
                  c.element().getValue().getFloat("avg_tx_bytes"),
                  c.element().getValue().getInt32("max_rx_bytes"),
                  c.element().getValue().getInt32("min_rx_bytes"),
                  c.element().getValue().getFloat("avg_rx_bytes"),
                  c.element().getValue().getInt32("max_duration"),
                  c.element().getValue().getInt32("min_duration"),
                  c.element().getValue().getFloat("avg_duration"))
              .build());
    }
  }
}
