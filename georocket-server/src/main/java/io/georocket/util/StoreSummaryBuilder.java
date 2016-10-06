package io.georocket.util;

import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;

/**
 * Helper class for {@link io.georocket.storage.Store}
 *
 * Build up a summary for a store and his layers.
 *
 * @author Andrej Sajenko
 */
public class StoreSummaryBuilder {
  private static Logger log = LoggerFactory.getLogger(StoreSummaryBuilder.class);
  private static final Date startDate = new Date(0L);
  private static final DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

  private JsonObject summary = new JsonObject();

  public StoreSummaryBuilder() {
  }

  /**
   * Add Chunk information‘s to add to the summary.
   *
   * @param fileName The full name of the stored file (including the layer as prefix.
   *                 The layer may be empty.)
   *                 The format must be "/layer/id" or "/id".
   * @param chunkSize The size of this chunk (file stored).
   * @param lastChange The last modification date of this chunk.
   *
   * @return this for fluent calls.
   */
  public StoreSummaryBuilder put(String fileName, int chunkSize, Date lastChange) {
    String layer = extractLayer(fileName);

    JsonObject layerInfo = summary.containsKey(layer)
      ? summary.getJsonObject(layer) : createEmptyInfo();

    layerInfo.put("size", layerInfo.getInteger("size") + chunkSize);
    layerInfo.put("lastChange", newestDate(
      layerInfo.getString("lastChange"), lastChange));
    layerInfo.put("chunkCount", layerInfo.getInteger("chunkCount") + 1);

    summary.put(layer, layerInfo);

    return this;
  }

  /**
   * @return The summary as a json object in the following structure
   * <code>
   * {
   *   "layer": {
   *     "size": number,
   *     "lastChange": "yyyy-MM-dd HH:mm:ss",
   *     "chunkCount": number
   *   },
   *   ...
   * }
   * </code>
   */
  public JsonObject finishBuilding() {
    return this.summary.copy();
  }

  private String extractLayer(String name) {
    // I expect exactly two types of names
    // 1) /layer/id
    // 2) /id
    // One with two slashes and one only with one.
    String parts[] = name.split("/");

    // TODO: currently the names are prefixed with "/store"
    // TODO: pull from georocket upstream master and decrement all numbers by one
    String layer = parts.length == 3
      ? "/" : parts.length == 4 ? "/" + parts[2] : null;

    if (layer == null) {
      log.error("Path splitted into " + Arrays.toString(parts));
    }

    return layer;
  }

  private String newestDate(String date1, Date date2) {
    Date dateA;
    try {
      dateA = df.parse(date1);
    } catch (ParseException e) {
      log.error("Could not parse date: '" + date1 + "', go on with unix start date", e);
      dateA = startDate;
    }
    Date date = dateA.before(date2) ? date2 : dateA;

    return df.format(date);
  }

  private JsonObject createEmptyInfo() {
    JsonObject o = new JsonObject();
    o.put("size", 0);
    o.put("lastChange", df.format(startDate));
    o.put("chunkCount", 0);
    return o;
  }
}
