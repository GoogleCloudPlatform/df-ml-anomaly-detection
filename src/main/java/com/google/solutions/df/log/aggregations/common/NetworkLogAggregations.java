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
import javax.annotation.Nullable;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;

@DefaultSchema(AutoValueSchema.class)
@AutoValue
public abstract class NetworkLogAggregations {
  @Nullable
  public abstract Long numberOfUniqueIPs();

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setNumberOfUniqueIPs(Long value);

    public abstract NetworkLogAggregations build();
  }

  public static Builder newBuilder() {
    return new AutoValue_NetworkLogAggregations.Builder();
  }
}
