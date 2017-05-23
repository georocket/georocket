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
public class TagsClient extends AbstractClient {
  /**
   * Construct a new tags client using the given HTTP client
   * @param client the HTTP client
   */
  public TagsClient(HttpClient client) {
    super(client);
  }

  /**
   * <p>Append tags to chunks in the GeoRocket data store.</p>
   * <p>The chunks are either specified by a <code>query</code> or
   * <code>layer</code> information or both. If none is given, the given
   * tags will be appended to all chunks in the data store.</p>
   * @param query a search query specifying the chunks, whose tags should
   * be updated (or <code>null</code> if the tags of all chunks in all
   * sub-layers from the given <code>layer</code> should be updated)
   * @param layer the absolute path to the layer from which to update tags
   * (or <code>null</code> if the entire store should be queried to find
   * the chunks, whose tags should be updated)
   * @param tags a collection of tags to be removed from the queried chunks
   * @param handler a handler that will be called when the operation has
   * finished
   * @since 1.1.0
   */
  public void appendTags(String query, String layer, List<String> tags,
    Handler<AsyncResult<Void>> handler) {
    update(HttpMethod.PUT, getEndpoint(), getFieldName(), query, layer,
      tags, handler);
  }

  /**
   * <p>Remove tags from chunks in the GeoRocket data store.</p>
   * <p>The chunks are either specified by a <code>query</code> or
   * <code>layer</code> information or both. If none is given, the given
   * tags will be removed from all chunks in the data store.</p>
   * @param query a search query specifying the chunks, whose tags should
   * be updated (or <code>null</code> if the tags of all chunks in all
   * sub-layers from the given <code>layer</code> should be updated)
   * @param layer the absolute path to the layer from which to update tags
   * (or <code>null</code> if the entire store should be queried to find
   * the chunks, whose tags should be updated)
   * @param tags a collection of tags to be removed from the queried chunks
   * @param handler a handler that will be called when the operation has
   * finished
   * @since 1.1.0
   */
  public void removeTags(String query, String layer, List<String> tags,
    Handler<AsyncResult<Void>> handler) {
    update(HttpMethod.DELETE, getEndpoint(), getFieldName(), query, layer,
      tags, handler);
  }

  /**
   * Return the query parameter name of the field
   * @return the field name
   */
  protected String getFieldName() {
    return "tags";
  }

  /**
   * Return the HTTP endpoint, the GeoRocket properties store path at
   * server side.
   * @return the endpoint
   */
  protected String getEndpoint() {
    return "/tags";
  }
}
