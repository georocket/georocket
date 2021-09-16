package io.georocket.index;

import com.google.common.collect.ImmutableList;
import com.google.common.io.Resources;
import io.georocket.constants.AddressConstants;
import io.georocket.constants.ConfigConstants;
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
   * A list of {@link IndexerFactory} objects
   */
  private List<? extends IndexerFactory> indexerFactories;

  /**
   * A function that extracts values of indexed attributes from Elasticsearch
   * source objects
   */
  private BiFunction<JsonObject, String, Stream<Object>> indexedAttributeExtractor;

  @Override
  public void start(Future<Void> startFuture) {
    // load and copy all indexer factories now and not lazily to avoid
    // concurrent modifications to the service loader's internal cache
    indexerFactories = ImmutableList.copyOf(FilteredServiceLoader.load(IndexerFactory.class));

    // create extractors for indexed attributes
    Map<String, Object> attributeMappings = new HashMap<>();
    indexerFactories.stream()
      .map(IndexerFactory::getIndexedAttributeMapping)
      .filter(Objects::nonNull)
      .forEach(mapping -> MapUtils.deepMerge(attributeMappings, mapping));
    indexedAttributeExtractor = attributeMappingsToExtractor(attributeMappings);

    startFuture.complete();
  }

  /**
   * Converts the given indexed attribute mapping to a function that extracts
   * the values of an indexed attribute from a Elasticsearch source object.
   * @param mapping the indexed attribute mapping
   * @return the function that extracts the values
   */
  private static BiFunction<JsonObject, String, Stream<Object>> attributeMappingsToExtractor(
    Map<String, Object> mapping) {
    return (JsonObject source, String key) -> {
      List<Object> values = new ArrayList<>();

      @SuppressWarnings("unchecked")
      Map<String, Object> properties = (Map<String, Object>)mapping.get("properties");
      if (properties != null && properties.containsKey(key)) {
        Object value = source.getValue(key);
        if (value != null) {
          values.add(value);
        }
      }

      @SuppressWarnings("unchecked")
      List<Map<String, Object>> dynamicMappings =
        (List<Map<String, Object>>)mapping.get("dynamic_templates");
      if (dynamicMappings != null) {
        for (Map<String, Object> dm : dynamicMappings) {
          for (Object templateObject : dm.values()) {
            @SuppressWarnings("unchecked")
            Map<String, Object> template = (Map<String, Object>)templateObject;
            String pathMatch = (String)template.get("path_match");
            if (pathMatch == null) {
              throw new IllegalStateException("Dynamic template for indexed " +
                "attribute must contain a path match");
            }

            if (!pathMatch.endsWith(".*")) {
              throw new IllegalStateException("Path match must end with `.*'");
            }

            String[] parts = pathMatch.split("\\.");
            JsonObject map = source;
            for (int i = 0; i < parts.length - 1; ++i) {
              map = map.getJsonObject(parts[i]);
              if (map == null) {
                break;
              }
            }

            if (map != null) {
              Object value = map.getString(key);
              if (value != null) {
                values.add(value);
              }
            }
          }
        }
      }

      return values.stream();
    };
  }

  /**
   * A function that can extract a value of a property from an Elasticsearch
   * source object
   * @param source the source object
   * @param key the property's name
   * @return the value as a list with a single element or an empty list if the
   * property does not exist
   */
  private static Stream<Object> propertyExtractor(JsonObject source, String key) {
    JsonObject map = source.getJsonObject("props");
    if (map == null) {
      return null;
    }
    String value = map.getString(key);
    if (value != null) {
      return Stream.of(value);
    } else {
      return Stream.empty();
    }
  }

  private Completable ensureMapping() {
    // merge mappings from all indexers
    Map<String, Object> mappings = new HashMap<>();
    indexerFactories.stream().filter(f -> f instanceof DefaultMetaIndexerFactory)
      .forEach(factory -> MapUtils.deepMerge(mappings, factory.getMapping()));
    indexerFactories.stream().filter(f -> !(f instanceof DefaultMetaIndexerFactory))
      .forEach(factory -> MapUtils.deepMerge(mappings, factory.getMapping()));

    // return client.putMapping(TYPE_NAME, new JsonObject(mappings)).toCompletable();
    return Completable.complete();
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

  private void registerCompletable(String address, Function<JsonObject, Completable> mapper) {
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
    return onGetMap(body, indexedAttributeExtractor, body.getString("attribute"));
  }

  private Single<JsonObject> onGetPropertyValues(JsonObject body) {
    return onGetMap(body, MetadataVerticle::propertyExtractor, body.getString("property"));
  }

  private Single<JsonObject> onGetMap(JsonObject body,
      BiFunction<JsonObject, String, Stream<Object>> extractor, String key) {
    return executeQuery(body)
      .map(result -> {
        JsonObject hits = result.getJsonObject("hits");

        List<Object> resultHits = hits.getJsonArray("hits").stream()
          .map(JsonObject.class::cast)
          .map(hit -> hit.getJsonObject("_source"))
          .flatMap(source -> extractor.apply(source, key))
          .collect(Collectors.toList());

        return new JsonObject()
          .put("hits", new JsonArray(resultHits))
          .put("totalHits", resultHits.size())
          .put("scrollId", result.getString("_scroll_id"));
      });
  }

  private Single<JsonObject> executeQuery(JsonObject body) {
    // String search = body.getString("search");
    // String path = body.getString("path");
    // String scrollId = body.getString("scrollId");
    // JsonObject parameters = new JsonObject()
    //   .put("size", body.getInteger("pageSize", 100));
    // String timeout = "1m"; // one minute
    //
    // if (scrollId == null) {
    //   try {
    //     // Execute a new search. Use a post_filter because we only want to get
    //     // a yes/no answer and no scoring (i.e. we only want to get matching
    //     // documents and not those that likely match). For the difference between
    //     // query and post_filter see the Elasticsearch documentation.
    //     JsonObject postFilter = queryCompiler.compileQuery(search, path);
    //     return client.beginScroll(TYPE_NAME, null, postFilter, parameters, timeout);
    //   } catch (Throwable t) {
    //     return Single.error(t);
    //   }
    // } else {
    //   // continue searching
    //   return client.continueScroll(scrollId, timeout);
    // }
    return Single.just(new JsonObject());
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
    JsonObject postFilter = new DefaultQueryCompiler(indexerFactories).compileQuery(search, path, null);

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
    // return client.updateByQuery(TYPE_NAME, postFilter, updateScript)
    //   .flatMapCompletable(sr -> {
    //     if (sr.getBoolean("timed_out", true)) {
    //       return Completable.error(new TimeoutException());
    //     }
    //     return Completable.complete();
    //   });
    return Completable.complete();
  }
}
