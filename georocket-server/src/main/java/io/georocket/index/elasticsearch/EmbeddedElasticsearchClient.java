package io.georocket.index.elasticsearch;

import java.util.List;

import org.jooq.lambda.tuple.Tuple2;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import rx.Completable;
import rx.Single;

/**
 * A client for embedded Elasticsearch instances. Will shut down the embedded
 * instance when the client is closed.
 * @author Michel Kraemer
 */
public class EmbeddedElasticsearchClient implements ElasticsearchClient {
  private final ElasticsearchClient delegate;
  private final ElasticsearchRunner runner;

  /**
   * Wrap around an existing {@link ElasticsearchClient} instance
   * @param delegate the client to wrap around
   * @param runner the Elasticsearch instance to stop when the client is closed
   */
  public EmbeddedElasticsearchClient(ElasticsearchClient delegate,
      ElasticsearchRunner runner) {
    this.delegate = delegate;
    this.runner = runner;
  }
  
  @Override
  public void close() {
    delegate.close();
    runner.stop();
  }

  @Override
  public Single<JsonObject> bulkInsert(List<Tuple2<String, JsonObject>> documents) {
    return delegate.bulkInsert(documents);
  }

  @Override
  public Single<JsonObject> beginScroll(JsonObject query, JsonObject postFilter,
      JsonObject aggregations, JsonObject parameters, String timeout) {
    return delegate.beginScroll(query, postFilter, aggregations, parameters, timeout);
  }

  @Override
  public Single<JsonObject> continueScroll(String scrollId, String timeout) {
    return delegate.continueScroll(scrollId, timeout);
  }

  @Override
  public Single<JsonObject> search(JsonObject query, JsonObject postFilter,
      JsonObject aggregations, JsonObject parameters) {
    return delegate.search(query, postFilter, aggregations, parameters);
  }

  @Override
  public Single<Long> count(JsonObject query) {
    return delegate.count(query);
  }

  @Override
  public Single<JsonObject> updateByQuery(JsonObject postFilter,
      JsonObject script) {
    return delegate.updateByQuery(postFilter, script);
  }

  @Override
  public Single<JsonObject> bulkDelete(JsonArray ids) {
    return delegate.bulkDelete(ids);
  }

  @Override
  public Single<Boolean> indexExists() {
    return delegate.indexExists();
  }

  @Override
  public Single<Boolean> createIndex() {
    return delegate.createIndex();
  }

  @Override
  public Single<Boolean> createIndex(JsonObject settings) {
    return delegate.createIndex(settings);
  }

  @Override
  public Completable ensureIndex() {
    return delegate.ensureIndex();
  }

  @Override
  public Single<Boolean> putMapping(JsonObject mapping) {
    return delegate.putMapping(mapping);
  }

  @Override
  public Single<JsonObject> getMapping() {
    return delegate.getMapping();
  }

  @Override
  public Single<JsonObject> getMapping(String field) {
    return delegate.getMapping(field);
  }
  
  @Override
  public Completable ensureMapping(JsonObject mapping) {
    return delegate.ensureMapping(mapping);
  }

  @Override
  public Single<Boolean> isRunning() {
    return delegate.isRunning();
  }
}
