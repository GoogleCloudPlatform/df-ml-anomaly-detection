package com.google.solutions.df.log.aggregations.common;

import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@DefaultSchema(AutoValueSchema.class)
@AutoValue
public abstract class BqRow {
  private static final Logger LOG = LoggerFactory.getLogger(BqRow.class);

  public abstract Integer subscriber_id();

  public abstract String dst_subnet();

  public abstract Integer number_of_unique_ips();

  public abstract Integer number_of_unique_ports();

  public abstract Integer number_of_records();

  public abstract Integer max_tx_bytes();

  public abstract Integer min_tx_bytes();

  public abstract Double avg_tx_bytes();

  public abstract Integer max_rx_bytes();

  public abstract Integer min_rx_bytes();

  public abstract Double avg_rx_bytes();

  public abstract Double avg_duration();

  public abstract Integer min_duration();

  public abstract Integer max_duration();

  public abstract String transaction_timestamp();

  abstract Builder toBuilder();

  public static Builder newBuilder() {
    return new AutoValue_BqRow.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setNumber_of_unique_ips(Integer numberOfIPs);

    public abstract Builder setSubscriber_id(Integer subscriberId);

    public abstract Builder setDst_subnet(String subnet);

    public abstract Builder setNumber_of_unique_ports(Integer numberOfPorts);

    public abstract Builder setNumber_of_records(Integer numberOfRecords);

    public abstract Builder setMax_tx_bytes(Integer maxTxBytes);

    public abstract Builder setMin_tx_bytes(Integer minTxBytes);

    public abstract Builder setAvg_tx_bytes(Double avgTxBytes);

    public abstract Builder setMax_rx_bytes(Integer maxRxBytes);

    public abstract Builder setMin_rx_bytes(Integer minRxBytes);

    public abstract Builder setAvg_rx_bytes(Double avgRxBytes);

    public abstract Builder setAvg_duration(Double avgDuration);

    public abstract Builder setMax_duration(Integer maxDuration);

    public abstract Builder setMin_duration(Integer minDuration);

    public abstract Builder setTransaction_timestamp(String timestamp);

    public abstract BqRow build();
  }
}
