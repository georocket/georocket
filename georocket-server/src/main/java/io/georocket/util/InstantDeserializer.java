package io.georocket.util;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;

import java.io.IOException;
import java.time.Instant;
import java.time.format.DateTimeFormatter;

/**
 * A JSON deserializer that converts ISO-8601 instant strings to {@link Instant}
 * objects. Vert.x is already able to convert {@link Instant} objects to Strings
 * while serializing JSON objects but not the other way round.
 * @author Michel Kraemer
 */
public class InstantDeserializer extends JsonDeserializer<Instant> {
  @Override
  public Instant deserialize(JsonParser jp, DeserializationContext ctxt)
      throws IOException {
    return Instant.from(DateTimeFormatter.ISO_INSTANT.parse(jp.getText()));
  }
}
