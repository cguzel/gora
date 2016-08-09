package org.apache.gora.couchdb.util;

import org.apache.avro.Schema;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.gora.util.IOUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class CouchDBIOUtils extends IOUtils {

  /**
   * Serializes the field object using the datumWriter.
   */
  public static<T> String serializeasJson(Schema schema, SpecificDatumWriter<T> datumWriter, T object)
      throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    JsonEncoder encoder = EncoderFactory.get().jsonEncoder(schema, out);
//    JsonEncoder encoder = EncoderFactory.get().jsonEncoder(schema,System.out);
    datumWriter.write(object, encoder);
    encoder.flush();
    return out.toString("UTF-8");
  }

}
