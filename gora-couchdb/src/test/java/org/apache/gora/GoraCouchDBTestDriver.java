/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.gora;

import org.apache.gora.couchdb.store.CouchDBParameters;
import org.apache.gora.couchdb.store.CouchDBStore;
import org.apache.gora.persistency.Persistent;
import org.apache.gora.store.DataStore;
import org.apache.gora.store.DataStoreFactory;
import org.apache.gora.util.GoraException;
import org.testcontainers.containers.GenericContainer;

import java.util.Properties;

public class GoraCouchDBTestDriver extends GoraTestDriver {

  private final GenericContainer couchdbContainer;
  private Properties properties = DataStoreFactory.createProps();

  /**
   * Default constructor
   */
  public GoraCouchDBTestDriver(GenericContainer couchdbContainer) {
    super(CouchDBStore.class);
    this.couchdbContainer = couchdbContainer;
  }

  @Override
  public void setUpClass() throws Exception {
    properties.put(CouchDBParameters.PROP_COUCHDB_PORT, couchdbContainer.getMappedPort(5984).toString());
  }

  @Override
  public <K, T extends Persistent> DataStore<K, T> createDataStore(Class<K> keyClass, Class<T> persistentClass)
      throws GoraException {

    final DataStore<K, T> dataStore = DataStoreFactory
        .createDataStore((Class<? extends DataStore<K, T>>) dataStoreClass, keyClass, persistentClass, conf,
            properties);
    dataStores.add(dataStore);

    log.info("Datastore for " + persistentClass + " was added.");
    return dataStore;
  }

}
