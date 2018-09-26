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
  public Single<JsonObject> bulkInsert(String type,
      List<Tuple2<String, JsonObject>> documents) {
    return delegate.bulkInsert(type, documents);
  }

  @Override
  public Single<JsonObject> beginScroll(String type, JsonObject query,
    JsonObject postFilter, JsonObject aggregations, JsonObject parameters, String timeout) {
    return delegate.beginScroll(type, query, postFilter, aggregations, parameters, timeout);
  }

  @Override
  public Single<JsonObject> continueScroll(String scrollId, String timeout) {
    return delegate.continueScroll(scrollId, timeout);
  }

  @Override
  public Single<JsonObject> search(String type, JsonObject query,
    JsonObject postFilter, JsonObject aggregations, JsonObject parameters) {
    return delegate.search(type, query, postFilter, aggregations, parameters);
  }

  @Override
  public Single<Long> count(String type, JsonObject query) {
    return delegate.count(type, query);
  }

  @Override
  public Single<JsonObject> updateByQuery(String type, JsonObject postFilter,
      JsonObject script) {
    return delegate.updateByQuery(type, postFilter, script);
  }

  @Override
  public Single<JsonObject> bulkDelete(String type, JsonArray ids) {
    return delegate.bulkDelete(type, ids);
  }

  @Override
  public Single<Boolean> indexExists() {
    return delegate.indexExists();
  }

  @Override
  public Single<Boolean> typeExists(String type) {
    return delegate.typeExists(type);
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
  public Single<Boolean> putMapping(String type, JsonObject mapping) {
    return delegate.putMapping(type, mapping);
  }

  @Override
  public Single<JsonObject> getMapping(String type) {
    return delegate.getMapping(type);
  }

  @Override
  public Single<JsonObject> getMapping(String type, String field) {
    return delegate.getMapping(type, field);
  }
  
  @Override
  public Completable ensureMapping(String type, JsonObject mapping) {
    return delegate.ensureMapping(type, mapping);
  }

  @Override
  public Single<Boolean> isRunning() {
    return delegate.isRunning();
  }
}
