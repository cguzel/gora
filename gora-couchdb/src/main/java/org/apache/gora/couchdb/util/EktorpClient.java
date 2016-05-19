package org.apache.gora.couchdb.util;

import org.ektorp.CouchDbConnector;
import org.ektorp.CouchDbInstance;
import org.ektorp.http.HttpClient;
import org.ektorp.http.StdHttpClient;
import org.ektorp.impl.StdCouchDbConnector;
import org.ektorp.impl.StdCouchDbInstance;

import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by cihat on 4/30/16.
 */
public class EktorpClient {
  public static void main(String[] args) throws MalformedURLException {
    HttpClient httpClient = new StdHttpClient.Builder()
        .url("http://localhost:5984")
        .build();

    CouchDbInstance dbInstance = new StdCouchDbInstance(httpClient);
    CouchDbConnector db = dbInstance.createConnector("mydatabase",true);

    db.createDatabaseIfNotExists();

    final List<String> countries = new ArrayList<>();
    countries.add("Turkey");
    countries.add("England");
    final List<String> citiesForTurkey= new ArrayList<>();
    citiesForTurkey.add("Istanbul");
    citiesForTurkey.add("Ankara");
    final List<String> citiesForEngland= new ArrayList<>();
    citiesForEngland.add("London");
    citiesForEngland.add("Manchaster");

    Map<String, List<String>> majorCitiesByCountry = new HashMap<>();
    majorCitiesByCountry.put("Turkey", citiesForTurkey);
    majorCitiesByCountry.put("England", citiesForEngland);

    final Map<String, Object> referenceData = new HashMap<String, Object>();
    referenceData.put("_id", "referenceData3");
    referenceData.put("countries", countries);
    referenceData.put("majorCitiesByCountry", majorCitiesByCountry);
    db.create(referenceData);

    Map<String, Object> referenceData_2 = db.get(Map.class, "referenceData3");

  }
}
