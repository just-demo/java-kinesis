package demo.util;

import java.io.IOException;
import java.io.UncheckedIOException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonUtils {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  public static String toJson(Object value) {
    try {
      return MAPPER.writeValueAsString(value);
    } catch (JsonProcessingException e) {
      throw new UncheckedIOException(e);
    }
  }

  public static <T> T fromJson(String value, Class<T> type) {
    try {
      return MAPPER.readValue(value, type);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
