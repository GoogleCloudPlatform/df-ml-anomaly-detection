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
package com.google.solutions.df.log.aggregations.common;

import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.transforms.AddFields;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("serial")
@AutoValue
public abstract class RawLogDataTransform extends PTransform<PCollection<Row>, PCollection<Row>> {
  public static final Logger LOG = LoggerFactory.getLogger(RawLogDataTransform.class);

  public abstract String dbPath();

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setDbPath(String path);

    public abstract RawLogDataTransform build();
  }

  public static Builder newBuilder() {
    return new AutoValue_RawLogDataTransform.Builder();
  }

  @Override
  public PCollection<Row> expand(PCollection<Row> input) {

    return input
        .apply("AddGeoColumn", 
        		AddFields.<Row>create()
        		.field("geoCountry", Schema.FieldType.STRING)
        		.field("geoCity", Schema.FieldType.STRING)
        		.field("latitude", Schema.FieldType.DOUBLE)
        		.field("longitude", Schema.FieldType.DOUBLE))
        .setRowSchema(Util.networkLogSchemaWithGeo)
        .apply("FindGeo", ParDo.of(new IpToGeoDoFn(dbPath())));
  }
}
