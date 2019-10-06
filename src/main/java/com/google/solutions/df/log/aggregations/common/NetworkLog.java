package com.google.solutions.df.log.aggregations.common;

import com.google.auto.value.AutoValue;
import javax.annotation.Nullable;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;

@DefaultSchema(AutoValueSchema.class)
@AutoValue
public abstract class NetworkLog {
  @Nullable
  public abstract String subscriberId();

  @Nullable
  public abstract String srcIP();

  @Nullable
  public abstract String dstIP();

  @Nullable
  public abstract Long srcPort();

  @Nullable
  public abstract Long dstPort();

  @Nullable
  public abstract Long txBytes();

  @Nullable
  public abstract Long rxBytes();

  @Nullable
  public abstract Long startTime();

  @Nullable
  public abstract Long endTime();

  @Nullable
  public abstract Long tcpFlag();

  @Nullable
  public abstract String protocolName();

  @Nullable
  public abstract Long protocolNumber();

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setSubscriberId(String value);

    public abstract Builder setSrcIP(String value);

    public abstract Builder setDstIP(String value);

    public abstract Builder setSrcPort(Long value);

    public abstract Builder setDstPort(Long value);

    public abstract Builder setTxBytes(Long value);

    public abstract Builder setRxBytes(Long value);

    public abstract Builder setStartTime(Long value);

    public abstract Builder setEndTime(Long value);

    public abstract Builder setTcpFlag(Long value);

    public abstract Builder setProtocolName(String value);

    public abstract Builder setProtocolNumber(Long value);

    public abstract NetworkLog build();
  }

  public static Builder newBuilder() {
    return new AutoValue_NetworkLog.Builder();
  }
}
