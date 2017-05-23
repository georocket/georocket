package io.georocket.client;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpMethod;

import java.util.List;

/**
 * A client connecting to the GeoRocket properties store
 * @since 1.1.0
 * @author Tim Hellhake
 */
public class PropertiesClient extends AbstractClient {
  /**
   * Construct a new properties client using the given HTTP client
   * @param client the HTTP client
   */
  public PropertiesClient(HttpClient client) {
    super(client);
  }
  
  /**
   * <p>Set properties of chunks in the GeoRocket data store. If a
   * property with the same key already exists, its value will be
   * overwritten.</p>.
   * <p>The chunks are either specified by a <code>query</code> or
   * <code>layer</code> information or both. If none is given, the given
   * properties will be appended to all chunks in the data store.</p>
   * @param query a search query specifying the chunks, whose
   * properties should be updated (or <code>null</code> if the
   * properties of all chunks in all sub-layers from the given
   * <code>layer</code> should be updated)
   * @param layer the absolute path to the layer in which to update
   * properties (or <code>null</code> if the entire store should be
   * queried to find the chunks, whose properties should be updated)
   * @param properties a collection of properties to set
   * @param handler a handler that will be called when the operation
   * has finished
   * @since 1.1.0
   */
  public void setProperties(String query, String layer, List<String> properties,
    Handler<AsyncResult<Void>> handler) {
    update(HttpMethod.PUT, getEndpoint(), getFieldName(), query, layer,
      properties, handler);
  }

  /**
   * <p>Remove properties from chunks in the GeoRocket data store.</p>
   * <p>The chunks are either specified by a <code>query</code> or
   * <code>layer</code> information or both. If none is given, the given
   * properties will be removed from all chunks in the data store.</p>
   * @param query a search query specifying the chunks, whose
   * properties should be updated (or <code>null</code> if the
   * properties of all chunks in all sub-layers from the given
   * <code>layer</code> should be updated)
   * @param layer the absolute path to the layer in which to update
   * properties (or <code>null</code> if the entire store should be
   * queried to find the chunks, whose properties should be updated)
   * @param properties a collection of properties to remove from
   * the queried chunks
   * @param handler a handler that will be called when the operation
   * has finished
   * @since 1.1.0
   */
  public void removeProperties(String query, String layer, List<String> properties,
    Handler<AsyncResult<Void>> handler) {
    update(HttpMethod.DELETE, getEndpoint(), getFieldName(), query, layer,
      properties, handler);
  }

  /**
   * Return the query parameter name of the field
   * @return the field name
   */
  protected String getFieldName() {
    return "properties";
  }

  /**
   * Return the HTTP endpoint, the GeoRocket properties store path at
   * server side.
   * @return the endpoint
   */
  protected String getEndpoint() {
    return "/properties";
  }
}
