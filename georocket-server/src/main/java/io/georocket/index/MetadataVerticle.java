package io.georocket.index;

import com.google.common.collect.ImmutableList;
import com.google.common.io.Resources;
import io.georocket.ServerAPIException;
import io.georocket.constants.AddressConstants;
import io.georocket.constants.ConfigConstants;
import io.georocket.index.elasticsearch.ElasticsearchClient;
import io.georocket.index.elasticsearch.ElasticsearchClientFactory;
import io.georocket.index.generic.DefaultMetaIndexerFactory;
import io.georocket.query.DefaultQueryCompiler;
import io.georocket.util.MapUtils;
import io.vertx.core.Future;
import io.vertx.core.impl.NoStackTraceThrowable;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.rxjava.core.AbstractVerticle;
import org.apache.commons.lang3.StringEscapeUtils;
import rx.Observable;
import rx.Single;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.ServiceLoader;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.regex.Pattern;
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
    register(AddressConstants.METADATA_UPDATE, this::onUpdate);
  }

  private <T> void register(String address,
    Function<JsonObject, Single<T>> mapper) {
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
    int pageSize = body.getInteger("pageSize", 100);
    String timeout = "1m"; // one minute

    if (scrollId == null) {
      try {
        // Execute a new search. Use a post_filter because we only want to get
        // a yes/no answer and no scoring (i.e. we only want to get matching
        // documents and not those that likely match). For the difference between
        // query and post_filter see the Elasticsearch documentation.
        JsonObject postFilter = queryCompiler.compileQuery(search, path, keyExists);
        return client.beginScroll(TYPE_NAME, null, postFilter, pageSize, timeout).toSingle();
      } catch (Throwable t) {
        return Single.error(t);
      }
    } else {
      // continue searching
      return client.continueScroll(scrollId, timeout).toSingle();
    }
  }

  /**
   * Remove or append meta data of existing chunks in the index. The chunks are
   * specified by a search query.
   * @param body the message containing the query and updates
   * @return an observable that emits a single item when the chunks have
   * been updated successfully
   */
  private Single<Void> onUpdate(JsonObject body) {
    String search = body.getString("search", "");
    String path = body.getString("path", "");
    JsonObject postFilter = queryCompiler.compileQuery(search, path);

    String action = body.getString("action", "").trim().toLowerCase();
    String target = body.getString("target", "").trim().toLowerCase();
    List<String> updates = body.getJsonArray("updates", new JsonArray())
      .stream()
      .map(x -> Objects.toString(x, ""))
      .collect(Collectors.toList());

    if (updates.isEmpty()) {
      return Single.error(new NoStackTraceThrowable(
        "Missing values to append or remove"));
    }

    JsonObject updateScript = new JsonObject()
      .put("lang", "painless");
    String scriptName = action + "_" + target + ".txt";

    try {
      JsonObject params;
      if (Objects.equals(scriptName, "set_properties.txt")) {
        params = new JsonObject().put(target, parseProperties(updates));
      } else {
        params = new JsonObject().put(target, new JsonArray(updates));
      }
      updateScript.put("params", params);

      URL url = getClass().getResource(scriptName);
      if (url == null) {
        throw new FileNotFoundException("Script " + scriptName + " does not exist");
      }
      String script = Resources.toString(url, StandardCharsets.UTF_8);
      updateScript.put("inline", script);
      return updateDocuments(postFilter, updateScript);
    } catch (ServerAPIException | IOException e) {
      return Single.error(e);
    }
  }

  /**
   * Parse list of properties in the form key:value
   * @param updates the list of properties
   * @return a json object with the property keys as object keys and the property
   * values as corresponding object values
   * @throws ServerAPIException if the syntax is not valid
   */
  private static JsonObject parseProperties(List<String> updates)
    throws ServerAPIException {
    JsonObject props = new JsonObject();
    String regex = "(?<!" + Pattern.quote("\\") + ")" + Pattern.quote(":");

    for (String part : updates) {
      part = part.trim();
      String[] property = part.split(regex);
      if (property.length != 2) {
        throw new ServerAPIException(
          ServerAPIException.INVALID_PROPERTY_SYNTAX_ERROR,
          "Invalid property syntax: " + part);
      }
      String key = StringEscapeUtils.unescapeJava(property[0].trim());
      String value = StringEscapeUtils.unescapeJava(property[1].trim());
      props.put(key, value);
    }

    return props;
  }

  /**
   * Update a document using a painless script
   * @param postFilter the filter to select the documents
   * @param updateScript the script which should be applied to the documents
   * @return a Single which completes if the update is successful or fails if
   * an error occurs
   */
  private Single<Void> updateDocuments(JsonObject postFilter, JsonObject updateScript) {
    return client.updateByQuery(TYPE_NAME, postFilter, updateScript)
      .toSingle()
      .flatMap(sr -> {
        if (sr.getBoolean("timed_out", true)) {
          return Single.error(new TimeoutException());
        }
        return Single.just(null);
      });
  }
}
