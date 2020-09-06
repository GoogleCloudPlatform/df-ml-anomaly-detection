package com.google.solutions.df.log.aggregations.common;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Resources;
import com.maxmind.db.CHMCache;
import com.maxmind.db.Reader.FileMode;
import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.exception.GeoIp2Exception;
import com.maxmind.geoip2.model.CountryResponse;

@SuppressWarnings("serial")
public class IpToGeoDoFn extends DoFn<Row,Row> {
	private static final Logger LOG = LoggerFactory.getLogger(IpToGeoDoFn.class);
	File file;
	DatabaseReader reader;
	@Setup
	public void setup() throws GeoIp2Exception, IOException {
    
	file = new File(Resources.getResource("GeoLite2-Country.mmdb").getPath());
	reader = new DatabaseReader.Builder(file)
				.fileMode(FileMode.MEMORY_MAPPED).withCache(new CHMCache()).build();
		
	}
	@Teardown
	public void tearDown() throws IOException {
		reader.close();
	}
	
	@ProcessElement
	public void processElement(ProcessContext c)   {
	
		String srcIP = c.element().getString("srcIP");
		try{
			CountryResponse country = reader.country(InetAddress.getByName(srcIP));
			LOG.info("Country {}",country.getCountry().getName());
			c.output(c.element());
		}catch(Exception e){
			c.output(c.element());
			LOG.error("Not found {}", srcIP);
		}
	}
}
