package org.apache.gora.couchdb.util;

import java.net.HttpURLConnection;
import java.net.URL;

public class HttpClient {

  public static HttpURLConnection getHttpConnection(String url, String type){
    HttpURLConnection con = null;
    try{
      final URL uri = new URL(url);
      con = (HttpURLConnection) uri.openConnection();
      con.setRequestMethod(type); //type: POST, PUT, DELETE, GET
    }catch(Exception e){
//      logger.info( "connection i/o failed" ); FIXME add a log
    }
    return con;
  }
}
