package com.google.solutions.df.log.aggregations.common;

import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.transforms.AddFields;
import org.apache.beam.sdk.schemas.transforms.Group;
import org.apache.beam.sdk.transforms.ApproximateUnique;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.Max;
import org.apache.beam.sdk.transforms.Min;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogRowTransform extends PTransform<PCollection<Row>, PCollection<Row>> {
  private static final Logger LOG = LoggerFactory.getLogger(LogRowTransform.class);
  private static final Integer SAMPLE_SIZE = 1000000;

  @Override
  public PCollection<Row> expand(PCollection<Row> row) {

    return row.apply(
            AddFields.<Row>create()
                .field("dstSubnet", Schema.FieldType.STRING)
                .field("duration", Schema.FieldType.INT32))
        .apply(MapElements.via(new LogAggrMapElement()))
        .apply(
            "Group By SubId & DestSubNet",
            Group.<Row>byFieldNames("subscriberId", "dstSubnet")
                .aggregateField(
                    "srcIP",
                    new ApproximateUnique.ApproximateUniqueCombineFn<String>(
                        SAMPLE_SIZE, StringUtf8Coder.of()),
                    "number_of_unique_ips")
                .aggregateField(
                    "srcPort",
                    new ApproximateUnique.ApproximateUniqueCombineFn<Integer>(
                        SAMPLE_SIZE, VarIntCoder.of()),
                    "number_of_unique_ports")
                .aggregateField("srcIP", Count.combineFn(), "number_of_records")
                .aggregateField("txBytes", new AvgCombineFn(), "avg_tx_bytes")
                .aggregateField("txBytes", Max.ofIntegers(), "max_tx_bytes")
                .aggregateField("txBytes", Min.ofIntegers(), "min_tx_bytes")
                .aggregateField("rxBytes", new AvgCombineFn(), "avg_rx_bytes")
                .aggregateField("rxBytes", Max.ofIntegers(), "max_rx_bytes")
                .aggregateField("rxBytes", Min.ofIntegers(), "min_rx_bytes")
                .aggregateField("duration", new AvgCombineFn(), "avg_duration")
                .aggregateField("duration", Max.ofIntegers(), "max_duration")
                .aggregateField("duration", Min.ofIntegers(), "min_duration"))
        .apply(Values.<Row>create())
        .apply(
            AddFields.<Row>create()
                .field("transaction_time", Schema.FieldType.STRING, Util.getTimeStamp()));
  }
}
