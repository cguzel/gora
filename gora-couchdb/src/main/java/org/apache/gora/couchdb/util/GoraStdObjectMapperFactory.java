package org.apache.gora.couchdb.util;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import org.ektorp.CouchDbConnector;
import org.ektorp.impl.ObjectMapperFactory;
import org.ektorp.impl.jackson.EktorpJacksonModule;
import org.ektorp.util.Assert;

public class GoraStdObjectMapperFactory implements ObjectMapperFactory {

  private ObjectMapper instance;
  private boolean writeDatesAsTimestamps = false;

  public synchronized ObjectMapper createObjectMapper() {
    if (instance == null) {
      instance = new ObjectMapper();
      applyDefaultConfiguration(instance);
    }
    return instance;
  }

  public synchronized ObjectMapper createObjectMapper(CouchDbConnector connector) {
    this.createObjectMapper();
    instance.registerModule(new EktorpJacksonModule(connector, instance));
    return instance;
  }

  private void applyDefaultConfiguration(ObjectMapper om) {
    om.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, this.writeDatesAsTimestamps);
    om.getSerializationConfig().withSerializationInclusion(JsonInclude.Include.NON_NULL);
  }

}