package com.google.solutions.df.log.aggregations.common.fraud.detection;

import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Util {

  public static final Logger LOG = LoggerFactory.getLogger(Util.class);

  public static TupleTag<Row> featureDataTag = new TupleTag<Row>() {};
  public static TupleTag<String> predictDataTag = new TupleTag<String>() {};
  public static final Integer keyRange = 100;
}
