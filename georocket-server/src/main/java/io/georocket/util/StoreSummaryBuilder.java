package io.georocket.util;

import io.vertx.core.json.JsonObject;

import java.util.Date;

/**
 * Helper class for {@link io.georocket.storage.Store}
 * Create a summary for a store and its layers.
 * @author Andrej Sajenko
 */
public class StoreSummaryBuilder {
  private static final long START_DATE = new Date(0L).getTime();
  private JsonObject summary = new JsonObject();

  /**
   * Create a store summary with a single entry for the root layer <code>/</code>.
   */
  public StoreSummaryBuilder() {
    summary.put("/", createEmptyInfo());
  }

  /**
   * Add chunk information to to the summary
   * @param layer the name of the layer containing the chunk. Valid
   * values are <code>/</code>, <code>/name</code> or
   * <code>/name/.../nameN</code> for example.
   * @param chunkSize the chunk's size
   * @param lastChange the chunk's last modification date
   * @param chunkCount the number of chunks stored in the same layer
   * @return <code>this</code> for fluent calls
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
   * Add chunk information to the summary. Assume that the layer
   * contains only one chunk.
   * @param layer the name of the layer containing the chunk. Valid
   * values are <code>/</code>, <code>/name</code> or
   * <code>/name/.../nameN</code> for example.
   * @param chunkSize the chunk's size
   * @param lastChange the chunk's last modification date
   * @return <code>this</code> for fluent calls
   */
  public StoreSummaryBuilder put(String layer, long chunkSize, long lastChange) {
    return put(layer, chunkSize, lastChange, 1);
  }

  /**
   * @return the summary as a JSON object in the following structure:
   * <pre><code>{
   *   "layer": {
   *     "size": number,
   *     "lastChange": number,
   *     "chunkCount": number
   *   },
   *   ...
   * }</code></pre>
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
    o.put("lastChange", START_DATE);
    o.put("chunkCount", 0);
    return o;
  }
}
