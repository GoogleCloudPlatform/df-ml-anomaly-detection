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
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.Row;
import org.apache.commons.math3.ml.distance.EuclideanDistance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@AutoValue
@SuppressWarnings("serial")
public abstract class PredictTransform extends PTransform<PCollection<Row>, PCollection<Row>> {
  public static final Logger LOG = LoggerFactory.getLogger(PredictTransform.class);
  public static final Double THRESH_HOLD_PARAM = 2.0;

  public abstract PCollectionView<List<CentroidVector>> centroidFeatureVector();

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setCentroidFeatureVector(PCollectionView<List<CentroidVector>> values);

    public abstract PredictTransform build();
  }

  public static Builder newBuilder() {
    return new AutoValue_PredictTransform.Builder();
  }

  @Override
  public PCollection<Row> expand(PCollection<Row> input) {

    return input
        .apply(
            "Distance From Nearest Centroid",
            ParDo.of(new FindEuclideanDistance(centroidFeatureVector()))
                .withSideInputs(centroidFeatureVector()))
        .setRowSchema(Util.outlierSchema)
        .apply(
            "Find Outliers(Z-Score)",
            ParDo.of(new CheckOutlierThreshold(centroidFeatureVector()))
                .withSideInputs(centroidFeatureVector()))
        .setRowSchema(Util.outlierSchema);
  }

  public static class CheckOutlierThreshold extends DoFn<Row, Row> {
    private PCollectionView<List<CentroidVector>> centroidFeature;

    public CheckOutlierThreshold(PCollectionView<List<CentroidVector>> centroidFeature) {
      this.centroidFeature = centroidFeature;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      Row aggrData = c.element();
      double[] inputVector = Util.getAggrVector(aggrData);

      CentroidVector centroidVector =
          c.sideInput(centroidFeature).stream()
              .filter(
                  centroid ->
                      centroid.centroidId().intValue()
                          == aggrData.getInt32("centroid_id").intValue())
              .findFirst()
              .orElse(null);

      if (centroidVector != null) {
        double normalizedDistance =
            Util.calculateStdDeviation(
                inputVector,
                centroidVector.featureVectors().stream().mapToDouble(d -> d).toArray());
        boolean outlierFound =
            (normalizedDistance / centroidVector.normalizedDistance()) > THRESH_HOLD_PARAM
                ? true
                : false;
        if (outlierFound) {
          c.output(aggrData);
          LOG.info(
              "*****Outlier Found*****- Centroid ID {}, Normalized Std Dev{}, InputFeature Std Dev {}",
              centroidVector.centroidId(),
              centroidVector.normalizedDistance(),
              normalizedDistance);
        }
      } else {
        LOG.info("Centroid Vector is null for centr {}", aggrData.getInt32("centroid_id"));
      }
    }
  }

  public static class FindEuclideanDistance extends DoFn<Row, Row> {
    private PCollectionView<List<CentroidVector>> centroidFeature;

    public FindEuclideanDistance(PCollectionView<List<CentroidVector>> centroidFeature) {
      this.centroidFeature = centroidFeature;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      HashMap<Integer, Double> centroidMap = new HashMap<Integer, Double>();
      double[] aggrFeatureVector = Util.getAggrVector(c.element());
      c.sideInput(centroidFeature)
          .forEach(
              feature -> {
                Double distanceFromCentroid =
                    new EuclideanDistance()
                        .compute(
                            aggrFeatureVector,
                            feature.featureVectors().stream().mapToDouble(d -> d).toArray());
                centroidMap.put(feature.centroidId(), distanceFromCentroid);

                LOG.debug(
                    "Centroid_id {} , distance {}", feature.centroidId(), distanceFromCentroid);
              });
      // centroid_id,
      Entry<Integer, Double> closestDistance =
          Collections.min(centroidMap.entrySet(), Comparator.comparing(Entry::getValue));

      LOG.debug(
          "****closet distance {}, centroid {}",
          closestDistance.getKey(),
          closestDistance.getValue());
      c.output(
          Row.withSchema(Util.outlierSchema)
              .addValues(
                  c.element().getString("subscriber_id"),
                  c.element().getString("dst_subnet"),
                  c.element().getString("transaction_time"),
                  c.element().getInt32("number_of_unique_ips"),
                  c.element().getInt32("number_of_unique_ports"),
                  c.element().getInt32("number_of_records"),
                  c.element().getInt32("max_tx_bytes"),
                  c.element().getInt32("min_tx_bytes"),
                  c.element().getDouble("avg_tx_bytes"),
                  c.element().getInt32("max_rx_bytes"),
                  c.element().getInt32("min_rx_bytes"),
                  c.element().getDouble("avg_rx_bytes"),
                  c.element().getInt32("max_duration"),
                  c.element().getInt32("min_duration"),
                  c.element().getDouble("avg_duration"),
                  closestDistance.getKey())
              .build());
    }
  }
}
