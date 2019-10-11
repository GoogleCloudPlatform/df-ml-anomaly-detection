package com.google.solutions.df.log.aggregations.common;

import com.google.auto.value.AutoValue;
import javax.annotation.Nullable;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@DefaultSchema(AutoValueSchema.class)
@AutoValue
public abstract class BqRow {
  private static final Logger LOG = LoggerFactory.getLogger(BqRow.class);

  public abstract Long number_of_unique_ips();

  public abstract Long number_of_unique_ports();

  public abstract Long number_of_records();

  public abstract Long max_tx_bytes();

  public abstract Long min_tx_bytes();

  public abstract Long avg_tx_bytes();

  public abstract Long max_rx_bytes();

  public abstract Long min_rx_bytes();

  public abstract Long avg_rx_bytes();

  public abstract Long avg_duration();

  public abstract Long min_duration();

  public abstract Long max_duration();

  @Nullable
  public abstract Long transaction_timestamp();

  abstract Builder toBuilder();

  public static Builder newBuilder() {
    return new AutoValue_BqRow.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setNumber_of_unique_ips(Long numberOfIPs);

    public abstract Builder setNumber_of_unique_ports(Long numberOfPorts);

    public abstract Builder setNumber_of_records(Long numberOfRecords);

    public abstract Builder setMax_tx_bytes(Long maxTxBytes);

    public abstract Builder setMin_tx_bytes(Long minTxBytes);

    public abstract Builder setAvg_tx_bytes(Long avgTxBytes);

    public abstract Builder setMax_rx_bytes(Long maxRxBytes);

    public abstract Builder setMin_rx_bytes(Long minRxBytes);

    public abstract Builder setAvg_rx_bytes(Long avgRxBytes);

    public abstract Builder setAvg_duration(Long avgDuration);

    public abstract Builder setMax_duration(Long maxDuration);

    public abstract Builder setMin_duration(Long minDuration);

    public abstract Builder setTransaction_timestamp(Long timestamp);

    public abstract BqRow build();
  }

  public BqRow withTimeStamp(Long timestamp) {
    return toBuilder().setTransaction_timestamp(timestamp).build();
  };
}
