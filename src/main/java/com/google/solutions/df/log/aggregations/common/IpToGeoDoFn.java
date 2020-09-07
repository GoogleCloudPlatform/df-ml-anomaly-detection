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

import com.google.cloud.ReadChannel;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.maxmind.db.CHMCache;
import com.maxmind.db.Reader.FileMode;
import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.exception.GeoIp2Exception;
import com.maxmind.geoip2.model.CountryResponse;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.URI;
import java.nio.channels.Channels;
import org.apache.beam.sdk.extensions.gcp.util.gcsfs.GcsPath;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("serial")
public class IpToGeoDoFn extends DoFn<Row, Row> {
  private static final Logger LOG = LoggerFactory.getLogger(IpToGeoDoFn.class);
  private DatabaseReader reader;
  private ReadChannel readerChannel;
  private InputStream inputStream;

  @Setup
  public void setup() throws GeoIp2Exception, IOException {
    GcsPath path =
        GcsPath.fromUri(URI.create("gs://df-ml-anomaly-detection-mock-data/GeoLite2-Country.mmdb"));
    Storage storage = StorageOptions.getDefaultInstance().getService();
    readerChannel = storage.reader(path.getBucket(), path.getObject());
    inputStream = Channels.newInputStream(readerChannel);
    reader =
        new DatabaseReader.Builder(inputStream)
            .fileMode(FileMode.MEMORY)
            .withCache(new CHMCache())
            .build();
  }

  @Teardown
  public void tearDown() throws IOException {
    if (reader != null) {
      reader.close();
      inputStream.close();
    }
  }

  @ProcessElement
  public void processElement(ProcessContext c) {

    String srcIP = c.element().getString("srcIP");
    try {
      CountryResponse country = reader.country(InetAddress.getByName(srcIP));
      LOG.info("Country {}", country.getCountry().getName());
      c.output(c.element());
    } catch (Exception e) {
      c.output(c.element());
      LOG.error("Not found {}", srcIP);
    }
  }
}
