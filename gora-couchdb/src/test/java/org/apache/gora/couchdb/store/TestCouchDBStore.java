/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.gora.couchdb.store;

import org.apache.avro.util.Utf8;
import org.apache.gora.GoraCouchDBTestDriver;
import org.apache.gora.examples.generated.Employee;
import org.apache.gora.examples.generated.WebPage;
import org.apache.gora.store.DataStore;
import org.apache.gora.store.DataStoreTestBase;
import org.apache.hadoop.conf.Configuration;
import org.junit.ClassRule;
import org.junit.Test;
import org.testcontainers.containers.GenericContainer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Tests extending {@link DataStoreTestBase}
 * which run the base JUnit test suite for Gora.
 */
public class TestCouchDBStore extends DataStoreTestBase {

  @ClassRule
  public static GenericContainer couchdb = new GenericContainer("klaemo/couchdb:1.6.1");

  static {
    try {
      setTestDriver(new GoraCouchDBTestDriver(couchdb));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private Configuration conf;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    conf = new Configuration();
  }

  @SuppressWarnings("unchecked")
  @Override
  protected DataStore<String, Employee> createEmployeeDataStore() throws IOException {
    throw new UnsupportedOperationException();
    //    return DataStoreFactory.createDataStore(CouchDBStore.class, String.class, Employee.class, conf);
  }

  @SuppressWarnings("unchecked")
  @Override
  protected DataStore<String, WebPage> createWebPageDataStore() throws IOException {
    throw new UnsupportedOperationException();
    //    return DataStoreFactory.createDataStore(CouchDBStore.class, String.class, WebPage.class, conf);
  }

  @Test
  public void testPutAndGet() {
    WebPage page = webPageStore.newPersistent();

    // Write webpage data
    page.setUrl(new Utf8("http://example.com"));
    byte[] contentBytes = "example content in example.com".getBytes(Charset.defaultCharset());
    ByteBuffer buff = ByteBuffer.wrap(contentBytes);
    page.setContent(buff);
    webPageStore.put("com.example/http", page);

    WebPage storedPage = webPageStore.get("com.example/http");

    assertNotNull(storedPage);
    assertEquals(page.getUrl(), storedPage.getUrl());
  }
}