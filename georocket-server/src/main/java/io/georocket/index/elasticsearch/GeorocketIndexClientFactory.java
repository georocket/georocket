package io.georocket.index.elasticsearch;

import io.georocket.index.IndexerFactory;
import io.georocket.index.generic.DefaultMetaIndexerFactory;
import io.georocket.util.MapUtils;
import io.vertx.core.json.JsonObject;
import io.vertx.rxjava.core.Vertx;
import rx.Observable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A factory for {@link ElasticsearchClient} instances with index and mapping
 * for georocket configured. See {@link ElasticsearchClientFactory}.
 * @author Tim Hellhake
 */
public class GeorocketIndexClientFactory {
  private final Vertx vertx;

  /**
   * Construct the factory
   * @param vertx the current Vert.x instance
   */
  public GeorocketIndexClientFactory(Vertx vertx) {
    this.vertx = vertx;
  }

  /**
   * Create an Elasticsearch client using
   * {@link ElasticsearchClientFactory#createElasticsearchClient(String)}
   * and ensure that index and mapping are configured.
   * @param indexName the name of the index the Elasticsearch client will
   * operate on
   * @param typeName the name of the type the mapping should be saved under
   * @param indexerFactories the list of indexer factories that should be used
   * to merge the mapping
   * @return an observable emitting an Elasticsearch client and runner
   */
  public Observable<ElasticsearchClient> createElasticsearchClient(
    String indexName, String typeName, List<? extends IndexerFactory> indexerFactories) {
    return new ElasticsearchClientFactory(vertx)
      .createElasticsearchClient(indexName)
      .flatMap(client -> client.ensureIndex().map(v -> client))
      .flatMap(client -> ensureMapping(client, typeName, indexerFactories));
  }

  /**
   * Ensure that the mappings off indexer factories are present in Elasticsearch
   * @param client the Elasticsearch client to use
   * @param typeName the name of the type the mapping should be saved under
   * @param indexerFactories the list of indexer factories that should be used
   * to merge the mapping
   * @return an observable emitting an Elasticsearch client and runner
   */
  private Observable<ElasticsearchClient> ensureMapping(ElasticsearchClient client,
    String typeName, List<? extends IndexerFactory> indexerFactories) {
    // merge mappings from all indexers
    Map<String, Object> mappings = new HashMap<>();
    indexerFactories.stream().filter(f -> f instanceof DefaultMetaIndexerFactory)
      .forEach(factory -> MapUtils.deepMerge(mappings, factory.getMapping()));
    indexerFactories.stream().filter(f -> !(f instanceof DefaultMetaIndexerFactory))
      .forEach(factory -> MapUtils.deepMerge(mappings, factory.getMapping()));

    return client.putMapping(typeName, new JsonObject(mappings)).map(r -> client);
  }
}
