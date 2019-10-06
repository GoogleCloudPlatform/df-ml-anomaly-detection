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

import org.apache.beam.sdk.transforms.Combine.CombineFn;

public class CountFn extends CombineFn<Long, Long, Long> {

  @Override
  public Long createAccumulator() {
    return 0L;
  }

  @Override
  public Long addInput(Long mutableAccumulator, Long input) {
    return mutableAccumulator + 1;
  }

  @Override
  public Long mergeAccumulators(Iterable<Long> accumulators) {
    long result = 0L;
    for (Long accum : accumulators) {
      result += accum;
    }
    return result;
  }

  @Override
  public Long extractOutput(Long accumulator) {
    return accumulator;
  }
}
