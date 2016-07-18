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
package org.apache.gora.couchdb.store;

import org.apache.hadoop.conf.Configuration;

import java.util.Properties;

public class CouchDBParameters {

  public static final String PROP_MAPPING_FILE = "gora.couchdb.mapping.file";

  public static final String PROP_COUCHDB_SERVER = "gora.datastore.couchdb.server";
  public static final String PROP_COUCHDB_PORT = "gora.datastore.couchdb.port";

  private final String mappingFile;
  private final String server;
  private final String port;

  private CouchDBParameters(String mappingFile, String server, String port) {
    this.mappingFile = mappingFile;
    this.server = server;
    this.port = port;
  }

  public String getMappingFile() {
    return mappingFile;
  }

  public String getServer() {
    return server;
  }

  public int getPort() {
    return Integer.parseInt(port);
  }

  public static CouchDBParameters load(Configuration conf) {
    String mappingFile = conf.get(PROP_MAPPING_FILE, CouchDBStore.DEFAULT_MAPPING_FILE);
    String couchDBServer = conf.get(PROP_COUCHDB_SERVER);
    String couchDBPort = conf.get(PROP_COUCHDB_PORT);

    return new CouchDBParameters(mappingFile, couchDBServer, couchDBPort);
  }
}
