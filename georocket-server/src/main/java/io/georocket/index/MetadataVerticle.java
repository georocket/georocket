package io.georocket.index;

import com.google.common.collect.ImmutableList;
import com.google.common.io.Resources;
import io.georocket.constants.AddressConstants;
import io.georocket.constants.ConfigConstants;
import io.georocket.index.elasticsearch.ElasticsearchClient;
import io.georocket.index.elasticsearch.ElasticsearchClientFactory;
import io.georocket.index.generic.DefaultMetaIndexerFactory;
import io.georocket.query.DefaultQueryCompiler;
import io.georocket.util.FilteredServiceLoader;
import io.georocket.util.MapUtils;
import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.rxjava.core.AbstractVerticle;
import rx.Completable;
import rx.Single;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.georocket.util.ThrowableHelper.throwableToCode;
import static io.georocket.util.ThrowableHelper.throwableToMessage;

/**
 * Generic methods for handling chunk metadata
 * @author Tim hellhake
 */
public class MetadataVerticle extends AbstractVerticle {
  private static Logger log = LoggerFactory.getLogger(IndexerVerticle.class);

  /**
   * Elasticsearch index
   */
  private static final String INDEX_NAME = "georocket";

  /**
   * Type of documents stored in the Elasticsearch index
   */
  private static final String TYPE_NAME = "object";

  /**
   * The Elasticsearch client
   */
  private ElasticsearchClient client;

  /**
   * Compiles search strings to Elasticsearch documents
   */
  private DefaultQueryCompiler queryCompiler;

  /**
   * A list of {@link IndexerFactory} objects
   */
  private List<? extends IndexerFactory> indexerFactories;

  @Override
  public void start(Future<Void> startFuture) {
    // load and copy all indexer factories now and not lazily to avoid
    // concurrent modifications to the service loader's internal cache
    indexerFactories = ImmutableList.copyOf(FilteredServiceLoader.load(IndexerFactory.class));
    queryCompiler = createQueryCompiler();
    queryCompiler.setQueryCompilers(indexerFactories);

    new ElasticsearchClientFactory(vertx).createElasticsearchClient(INDEX_NAME)
      .doOnSuccess(es -> {
        client = es;
      })
      .flatMapCompletable(v -> client.ensureIndex())
      .andThen(Completable.defer(() -> ensureMapping()))
      .subscribe(() -> {
        registerMessageConsumers();
        startFuture.complete();
      }, startFuture::fail);
  }

  @Override
  public void stop() {
    client.close();
  }

  private DefaultQueryCompiler createQueryCompiler() {
    JsonObject config = vertx.getOrCreateContext().config();
    String cls = config.getString(ConfigConstants.QUERY_COMPILER_CLASS,
      DefaultQueryCompiler.class.getName());
    try {
      return (DefaultQueryCompiler)Class.forName(cls).newInstance();
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException("Could not create a DefaultQueryCompiler", e);
    }
  }

  private Completable ensureMapping() {
    // merge mappings from all indexers
    Map<String, Object> mappings = new HashMap<>();
    indexerFactories.stream().filter(f -> f instanceof DefaultMetaIndexerFactory)
      .forEach(factory -> MapUtils.deepMerge(mappings, factory.getMapping()));
    indexerFactories.stream().filter(f -> !(f instanceof DefaultMetaIndexerFactory))
      .forEach(factory -> MapUtils.deepMerge(mappings, factory.getMapping()));

    return client.putMapping(TYPE_NAME, new JsonObject(mappings)).toCompletable();
  }

  /**
   * Register all message consumers for this verticle
   */
  private void registerMessageConsumers() {
    register(AddressConstants.METADATA_GET_ATTRIBUTE_VALUES, this::onGetAttributeValues);
    register(AddressConstants.METADATA_GET_PROPERTY_VALUES, this::onGetPropertyValues);
    registerCompletable(AddressConstants.METADATA_SET_PROPERTIES, this::onSetProperties);
    registerCompletable(AddressConstants.METADATA_REMOVE_PROPERTIES, this::onRemoveProperties);
    registerCompletable(AddressConstants.METADATA_APPEND_TAGS, this::onAppendTags);
    registerCompletable(AddressConstants.METADATA_REMOVE_TAGS, this::onRemoveTags);
  }

  private <T> void registerCompletable(String address, Function<JsonObject, Completable> mapper) {
    register(address, obj -> mapper.apply(obj).toSingleDefault(0));
  }

  private <T> void register(String address, Function<JsonObject, Single<T>> mapper) {
    vertx.eventBus().<JsonObject>consumer(address)
      .toObservable()
      .subscribe(msg -> {
        mapper.apply(msg.body()).subscribe(msg::reply, err -> {
          log.error("Could not perform query", err);
          msg.fail(throwableToCode(err), throwableToMessage(err, ""));
        });
      });
  }

  private Single<JsonObject> onGetAttributeValues(JsonObject body) {
    return onGetMap(body, "genAttrs", body.getString("attribute"));
  }

  private Single<JsonObject> onGetPropertyValues(JsonObject body) {
    return onGetMap(body, "props", body.getString("property"));
  }

  private Single<JsonObject> onGetMap(JsonObject body, String map, String key) {
    return executeQuery(body, map + "." + key)
      .map(result -> {
        JsonObject hits = result.getJsonObject("hits");

        List<String> resultHits = hits.getJsonArray("hits").stream()
          .map(JsonObject.class::cast)
          .map(hit -> hit.getJsonObject("_source"))
          .flatMap(source -> source.getJsonObject(map, new JsonObject()).stream())
          .filter(pair -> Objects.equals(pair.getKey(), key))
          .map(Map.Entry::getValue)
          .map(String.class::cast)
          .collect(Collectors.toList());

        return new JsonObject()
          .put("hits", new JsonArray(resultHits))
          .put("totalHits", hits.getLong("total"))
          .put("scrollId", result.getString("_scroll_id"));
      });
  }

  private Single<JsonObject> executeQuery(JsonObject body, String keyExists) {
    String search = body.getString("search");
    String path = body.getString("path");
    String scrollId = body.getString("scrollId");
    JsonObject parameters = new JsonObject()
      .put("size", body.getInteger("pageSize", 100));
    String timeout = "1m"; // one minute

    if (scrollId == null) {
      try {
        // Execute a new search. Use a post_filter because we only want to get
        // a yes/no answer and no scoring (i.e. we only want to get matching
        // documents and not those that likely match). For the difference between
        // query and post_filter see the Elasticsearch documentation.
        JsonObject postFilter = queryCompiler.compileQuery(search, path, keyExists);
        return client.beginScroll(TYPE_NAME, null, postFilter, parameters, timeout);
      } catch (Throwable t) {
        return Single.error(t);
      }
    } else {
      // continue searching
      return client.continueScroll(scrollId, timeout);
    }
  }

  /**
   * Set properties of a list of chunks
   * @param body the message containing the search, path and the properties
   * @return a Completable that will complete when the properties have been set
   * successfully
   */
  private Completable onSetProperties(JsonObject body) {
    JsonObject list = body.getJsonObject("properties");
    JsonObject params = new JsonObject().put("properties", list);
    return updateMetadata(body, "set_properties.txt", params);
  }

  /**
   * Remove properties of a list of chunks
   * @param body the message containing the search, path and the properties
   * @return a Completable that will complete when the properties have been
   * deleted successfully
   */
  private Completable onRemoveProperties(JsonObject body) {
    JsonArray list = body.getJsonArray("properties");
    JsonObject params = new JsonObject().put("properties", list);
    return updateMetadata(body, "remove_properties.txt", params);
  }

  /**
   * Append tags to a list of chunks
   * @param body the message containing the search, path and the tags
   * @return a Completable that will complete when the tags have been set
   * successfully
   */
  private Completable onAppendTags(JsonObject body) {
    JsonArray list = body.getJsonArray("tags");
    JsonObject params = new JsonObject().put("tags", list);
    return updateMetadata(body, "append_tags.txt", params);
  }

  /**
   * Remove tags of a list of chunks
   * @param body the message containing the search, path and the tags
   * @return a Completable that will complete when the tags have been set
   * successfully
   */
  private Completable onRemoveTags(JsonObject body) {
    JsonArray list = body.getJsonArray("tags");
    JsonObject params = new JsonObject().put("tags", list);
    return updateMetadata(body, "remove_tags.txt", params);
  }

  /**
   * Update the meta data of existing chunks in the index. The chunks are
   * specified by a search query.
   * @param body the message containing the search and path
   * @param scriptName the name of the painscript file
   * @param params the parameters for the painscript
   * @return a Completable that will complete when the chunks have been updated
   * successfully
   */
  private Completable updateMetadata(JsonObject body, String scriptName,
    JsonObject params) {
    String search = body.getString("search", "");
    String path = body.getString("path", "");
    JsonObject postFilter = queryCompiler.compileQuery(search, path);

    JsonObject updateScript = new JsonObject()
      .put("lang", "painless");

    try {
      updateScript.put("params", params);

      URL url = getClass().getResource(scriptName);
      if (url == null) {
        throw new FileNotFoundException("Script " + scriptName + " does not exist");
      }
      String script = Resources.toString(url, StandardCharsets.UTF_8);
      updateScript.put("inline", script);
      return updateDocuments(postFilter, updateScript);
    } catch (IOException e) {
      return Completable.error(e);
    }
  }

  /**
   * Update a document using a painless script
   * @param postFilter the filter to select the documents
   * @param updateScript the script which should be applied to the documents
   * @return a Completable that completes if the update is successful or fails
   * if an error occurs
   */
  private Completable updateDocuments(JsonObject postFilter, JsonObject updateScript) {
    return client.updateByQuery(TYPE_NAME, postFilter, updateScript)
      .flatMapCompletable(sr -> {
        if (sr.getBoolean("timed_out", true)) {
          return Completable.error(new TimeoutException());
        }
        return Completable.complete();
      });
  }
}
