package io.georocket.index.elasticsearch;

import java.util.Map;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import rx.Observable;

/**
 * An Elasticsearch client
 * @author Michel Kraemer
 */
public interface ElasticsearchClient {
  /**
   * Close the client and release all resources
   */
  void close();
  
  /**
   * Insert a number of documents in one bulk request
   * @param type the type of the documents to insert
   * @param documents maps document IDs to the actual documents to insert
   * @return the parsed bulk response from the server
   * @see #bulkResponseHasErrors(JsonObject)
   * @see #bulkResponseGetErrorMessage(JsonObject)
   */
  Observable<JsonObject> bulkInsert(String type, Map<String, JsonObject> documents);
  
  /**
   * Perform a search and start scrolling over the result documents
   * @param type the type of the documents to search
   * @param query the query to send
   * @param pageSize the number of objects to return in one response
   * @param timeout the time after which the returned scroll id becomes invalid
   * @return an object containing the search hits and a scroll id that can
   * be passed to {@link #continueScroll(String, String)} to get more results
   */
  default Observable<JsonObject> beginScroll(String type, JsonObject query,
      int pageSize, String timeout) {
    return beginScroll(type, query, null, pageSize, timeout);
  }
  
  /**
   * Perform a search and start scrolling over the result documents. You can
   * either specify a <code>query</code>, a <code>postFilter</code> or both,
   * but one of them is required.
   * @param type the type of the documents to search
   * @param query the query to send (may be <code>null</code>, in this case
   * <code>postFilter</code> must be set)
   * @param postFilter a filter to apply (may be <code>null</code>, in this case
   * <code>query</code> must be set)
   * @param pageSize the number of objects to return in one response
   * @param timeout the time after which the returned scroll id becomes invalid
   * @return an object containing the search hits and a scroll id that can
   * be passed to {@link #continueScroll(String, String)} to get more results
   */
  default Observable<JsonObject> beginScroll(String type, JsonObject query,
      JsonObject postFilter, int pageSize, String timeout) {
    return beginScroll(type, query, postFilter, null, pageSize, timeout);
  }

  /**
   * Perform a search, apply an aggregation, and start scrolling over the
   * result documents. You can either specify a <code>query</code>, a
   * <code>postFilter</code> or both, but one of them is required.
   * @param type the type of the documents to search
   * @param query the query to send (may be <code>null</code>, in this case
   * <code>postFilter</code> must be set)
   * @param postFilter a filter to apply (may be <code>null</code>, in this case
   * <code>query</code> must be set)
   * @param aggregation the aggregation to apply. Can be <code>null</code>
   * @param pageSize the number of objects to return in one response
   * @param timeout the time after which the returned scroll id becomes invalid
   * @return an object containing the search hits and a scroll id that can
   * be passed to {@link #continueScroll(String, String)} to get more results
   */
  Observable<JsonObject> beginScroll(String type, JsonObject query,
      JsonObject postFilter, JsonObject aggregation, int pageSize, String timeout);
  
  /**
   * Continue scrolling through search results. Call
   * {@link #beginScroll(String, JsonObject, int, String)} to get a scroll id
   * @param scrollId the scroll id
   * @param timeout the time after which the scroll id becomes invalid
   * @return an object containing new search hits and possibly a new scroll id
   */
  Observable<JsonObject> continueScroll(String scrollId, String timeout);
  
  /**
   * Delete a number of documents in one bulk request
   * @param type the type of the documents to delete
   * @param ids the IDs of the documents to delete
   * @return the parsed bulk response from the server
   * @see #bulkResponseHasErrors(JsonObject)
   * @see #bulkResponseGetErrorMessage(JsonObject)
   */
  Observable<JsonObject> bulkDelete(String type, JsonArray ids);

  /**
   * Check if the index exists
   * @return an observable emitting <code>true</code> if the index exists or
   * <code>false</code> otherwise
   */
  Observable<Boolean> indexExists();

  /**
   * Check if the type of the index exists
   * @param type the type
   * @return an observable emitting <code>true</code> if the type of
   * the index exists or <code>false</code> otherwise
   */
  Observable<Boolean> typeExists(String type);

  /**
   * Create the index
   * @return an observable emitting <code>true</code> if the index creation
   * was acknowledged by Elasticsearch, <code>false</code> otherwise
   */
  Observable<Boolean> createIndex();
  
  /**
   * Add mapping for the given type
   * @param type the type
   * @param mapping the mapping to set for the index
   * @return an observable emitting <code>true</code> if the operation
   * was acknowledged by Elasticsearch, <code>false</code> otherwise
   */
  Observable<Boolean> putMapping(String type, JsonObject mapping);
  
  /**
   * Check if Elasticsearch is running and if it answers to a simple request
   * @return <code>true</code> if Elasticsearch is running, <code>false</code>
   * otherwise
   */
  Observable<Boolean> isRunning();

  /**
   * Check if the given bulk response contains errors
   * @param response the bulk response
   * @return <code>true</code> if the response has errors, <code>false</code>
   * otherwise
   * @see #bulkInsert(String, Map)
   * @see #bulkDelete(String, JsonArray)
   * @see #bulkResponseGetErrorMessage(JsonObject)
   */
  default boolean bulkResponseHasErrors(JsonObject response) {
    return response.getBoolean("errors", false);
  }
  
  /**
   * Builds an error message from a bulk response containing errors
   * @param response the response containing errors
   * @return the error message or null if the bulk response does not contain
   * errors
   * @see #bulkInsert(String, Map)
   * @see #bulkDelete(String, JsonArray)
   * @see #bulkResponseHasErrors(JsonObject)
   */
  default String bulkResponseGetErrorMessage(JsonObject response) {
    if (!bulkResponseHasErrors(response)) {
      return null;
    }
    
    StringBuilder res = new StringBuilder();
    res.append("Errors in bulk operation:");
    JsonArray items = response.getJsonArray("items", new JsonArray());
    for (Object o : items) {
      JsonObject jo = (JsonObject)o;
      for (String key : jo.fieldNames()) {
        JsonObject op = jo.getJsonObject(key);
        if (bulkResponseItemHasErrors(op)) {
          res.append(bulkResponseItemGetErrorMessage(op));
        }
      }
    }
    return res.toString();
  }
  
  /**
   * Check if an item of a bulk response has an error
   * @param item the item
   * @return <code>true</code> if the item has an error, <code>false</code>
   * otherwise
   */
  default boolean bulkResponseItemHasErrors(JsonObject item) {
    return item.getJsonObject("error") != null;
  }
  
  /**
   * Builds an error message from a bulk response item
   * @param item the item
   * @return the error message or null if the bulk response item does not
   * contain an error
   */
  default String bulkResponseItemGetErrorMessage(JsonObject item) {
    if (!bulkResponseItemHasErrors(item)) {
      return null;
    }
    
    StringBuilder res = new StringBuilder();
    JsonObject error = item.getJsonObject("error");
    if (error != null) {
      String id = item.getString("_id");
      String type = error.getString("type");
      String reason = error.getString("reason");
      res.append("\n[id: [");
      res.append(id);
      res.append("], type: [");
      res.append(type);
      res.append("], reason: [");
      res.append(reason);
      res.append("]]");
    }
    
    return res.toString();
  }
}
