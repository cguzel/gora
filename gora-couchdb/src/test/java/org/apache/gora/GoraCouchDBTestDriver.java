/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.gora;

import org.apache.gora.couchdb.store.CouchDBMapping;
import org.apache.gora.couchdb.store.CouchDBStore;
import org.ektorp.CouchDbConnector;
import org.ektorp.CouchDbInstance;


/**
 * Helper class for third part tests using gora-couchdb backend.
 * @see GoraTestDriver
 */
public class GoraCouchDBTestDriver extends GoraTestDriver {

  /**
   * Data store to be used within the test driver
   */
//  private static DataStore<String,> personStore;

  private CouchDBMapping mapping;
  private CouchDbInstance dbInstance;
  private CouchDbConnector db; //FIXME burada connection kapatman lazım

  /**
   * Default constructor
   */
  public GoraCouchDBTestDriver() {
    super(CouchDBStore.class);
  }

  /**
   * Sets up the class
   */
  @Override
  public void setUpClass() throws Exception {
    super.setUpClass();
    log.info("Initializing CouchDB.");
  }

//  /**
//   * Creates the CouchDB store and returns an specific object
//   *
//   * @return
//   * @throws IOException
//   */
//  @SuppressWarnings("unchecked")
//  protected <String, T extends Persistent> DataStore<String, Class<T> persistentClass> createDataStore()
//      throws IOException {
//    if (personStore == null)
//      personStore = WSDataStoreFactory.createDataStore(CouchDBStore.class,
//          String.class, persistentClass , null);
//    return personStore;
//  }
//
//  /**
//   * Creates the CouchDB store but returns a generic object
//   */
//  @SuppressWarnings("unchecked")
//  public <K, T extends Persistent> DataStore<K, T> createDataStore(
//      Class<K> keyClass, Class<T> persistentClass) throws GoraException {
//    personStore = (CouchDBStore<String, Person>) WSDataStoreFactory
//        .createDataStore((Class<? extends DataStore<K, T>>) dataStoreClass,
//            keyClass, persistentClass, null);
//    dataStores.add(personStore);
//    return (DataStore<K, T>) personStore;
//  }
//
//  /**
//   * Gets or create the CouchDB data store
//   *
//   * @return
//   */
//  public DataStore<String, Person> getDataStore(){
//    try {
//      if(personStore != null)
//        return personStore;
//      else
//        return createDataStore();
//    } catch (IOException e) {
//      throw new RuntimeException(e);
//    }
//  }

}
