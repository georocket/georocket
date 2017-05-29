package io.georocket.index;

import com.google.common.collect.ImmutableList;
import io.georocket.constants.AddressConstants;
import io.georocket.constants.ConfigConstants;
import io.georocket.index.elasticsearch.ElasticsearchClient;
import io.georocket.index.elasticsearch.ElasticsearchClientFactory;
import io.georocket.index.generic.DefaultMetaIndexerFactory;
import io.georocket.query.DefaultQueryCompiler;
import io.georocket.util.MapUtils;
import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.rxjava.core.AbstractVerticle;
import rx.Observable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.ServiceLoader;
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
    indexerFactories = ImmutableList.copyOf(ServiceLoader.load(IndexerFactory.class));
    queryCompiler = createQueryCompiler();
    queryCompiler.setQueryCompilers(indexerFactories);

    new ElasticsearchClientFactory(vertx).createElasticsearchClient(INDEX_NAME)
      .doOnNext(es -> {
        client = es;
      })
      .flatMap(v -> client.ensureIndex())
      .flatMap(v -> ensureMapping())
      .subscribe(es -> {
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

  private Observable<Void> ensureMapping() {
    // merge mappings from all indexers
    Map<String, Object> mappings = new HashMap<>();
    indexerFactories.stream().filter(f -> f instanceof DefaultMetaIndexerFactory)
      .forEach(factory -> MapUtils.deepMerge(mappings, factory.getMapping()));
    indexerFactories.stream().filter(f -> !(f instanceof DefaultMetaIndexerFactory))
      .forEach(factory -> MapUtils.deepMerge(mappings, factory.getMapping()));

    return client.putMapping(TYPE_NAME, new JsonObject(mappings)).map(r -> null);
  }

  /**
   * Register all message consumers for this verticle
   */
  private void registerMessageConsumers() {
    register(AddressConstants.METADATA_GET_PROPERTIES, this::onGetPropertyValues);
  }

  private void register(String address,
                        Function<JsonObject, Observable<JsonObject>> mapper) {
    vertx.eventBus().<JsonObject>consumer(address)
      .toObservable()
      .subscribe(msg -> {
        mapper.apply(msg.body()).subscribe(msg::reply,
          err -> {
            log.error("Could not perform query", err);
            msg.fail(throwableToCode(err), throwableToMessage(err, ""));
          });
      });
  }

  private Observable<JsonObject> onGetPropertyValues(JsonObject body) {
    return onGetMap(body, "props", body.getString("property"));
  }

  private Observable<JsonObject> onGetMap(JsonObject body, String map, String key) {
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

  private Observable<JsonObject> executeQuery(JsonObject body, String keyExists) {
    String search = body.getString("search");
    String path = body.getString("path");
    String scrollId = body.getString("scrollId");
    int pageSize = body.getInteger("pageSize", 100);
    String timeout = "1m"; // one minute

    if (scrollId == null) {
      try {
        // Execute a new search. Use a post_filter because we only want to get
        // a yes/no answer and no scoring (i.e. we only want to get matching
        // documents and not those that likely match). For the difference between
        // query and post_filter see the Elasticsearch documentation.
        JsonObject postFilter = queryCompiler.compileQuery(search, path, keyExists);
        return client.beginScroll(TYPE_NAME, null, postFilter, pageSize, timeout);
      } catch (Throwable t) {
        return Observable.error(t);
      }
    } else {
      // continue searching
      return client.continueScroll(scrollId, timeout);
    }
  }
}
