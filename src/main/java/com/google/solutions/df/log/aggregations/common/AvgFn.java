package com.google.solutions.df.log.aggregations.common;

import java.io.Serializable;
import org.apache.beam.sdk.transforms.Combine.CombineFn;

public class AvgFn extends CombineFn<Long, AvgFn.Accum, Double> {

  public static class Accum implements Serializable {
    long sum = 0;
    long count = 0;

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + (int) (count ^ (count >>> 32));
      result = prime * result + (int) (sum ^ (sum >>> 32));
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) return true;
      if (obj == null) return false;
      if (getClass() != obj.getClass()) return false;
      Accum other = (Accum) obj;
      if (count != other.count) return false;
      if (sum != other.sum) return false;
      return true;
    }
  }

  @Override
  public Accum createAccumulator() {
    return new Accum();
  }

  @Override
  public Accum addInput(Accum mutableAccumulator, Long input) {
    mutableAccumulator.sum += input;
    mutableAccumulator.count++;
    return mutableAccumulator;
  }

  @Override
  public Accum mergeAccumulators(Iterable<Accum> accumulators) {
    Accum merged = createAccumulator();
    for (Accum accum : accumulators) {
      merged.sum += accum.sum;
      merged.count += accum.count;
    }
    return merged;
  }

  @Override
  public Double extractOutput(Accum accumulator) {
    return ((double) accumulator.sum) / accumulator.count;
  }
}
