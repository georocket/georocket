package io.georocket.util;

import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  private static final long startDate = new Date(0L).getTime();

  private JsonObject summary = new JsonObject();

  public StoreSummaryBuilder() {
  }

  /**
   * Add Chunk information‘s to add to the summary.
   *
   * @param layer The neme of the layer. May be / or /name.
   * @param chunkSize The size of this chunk (file stored).
   * @param lastChange The last modification date of this chunk.
   * @param chunkCount The number of chunks stored in this layer.
   *
   * @return this for fluent calls.
   */
  public StoreSummaryBuilder put(String layer, long chunkSize, long lastChange,
                                 int chunkCount) {
    JsonObject layerInfo = summary.containsKey(layer)
      ? summary.getJsonObject(layer) : createEmptyInfo();

    layerInfo.put("size", layerInfo.getLong("size") + chunkSize);
    layerInfo.put("lastChange", newestDate(
      layerInfo.getLong("lastChange"), lastChange));
    layerInfo.put("chunkCount", layerInfo.getInteger("chunkCount") + chunkCount);

    summary.put(layer, layerInfo);

    return this;
  }

  /**
   * Add Chunk information‘s to add to the summary.
   * Will assume that the layer information have one chunk stored.
   *
   * @param layer The neme of the layer. May be / or /name.
   * @param chunkSize The size of this chunk (file stored).
   * @param lastChange The last modification date of this chunk.
   *
   * @return this for fluent calls.
   */
  public StoreSummaryBuilder put(String layer, long chunkSize, long lastChange) {
    return put(layer, chunkSize, lastChange, 1);
  }

  /**
   * @return The summary as a json object in the following structure
   * <code>
   * {
   *   "layer": {
   *     "size": number,
   *     "lastChange": number,
   *     "chunkCount": number
   *   },
   *   ...
   * }
   * </code>
   */
  public JsonObject finishBuilding() {
    return this.summary.copy();
  }

  private long newestDate(long date1, long date2) {
    return date1 > date2 ? date1 : date2;
  }

  private JsonObject createEmptyInfo() {
    JsonObject o = new JsonObject();
    o.put("size", 0L);
    o.put("lastChange", startDate);
    o.put("chunkCount", 0);
    return o;
  }
}
