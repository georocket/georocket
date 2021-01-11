package io.georocket.index.elasticsearch;

import io.georocket.constants.ConfigConstants;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import rx.Single;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;

/**
 * A factory for {@link ElasticsearchClient} instances. Either starts an
 * Elasticsearch instance or connects to an external one - depending on the
 * configuration.
 * @author Michel Kraemer
 */
public class ElasticsearchClientFactory {
  private static Logger log = LoggerFactory.getLogger(ElasticsearchClientFactory.class);

  private final Vertx vertx;
  
  /**
   * Construct the factory
   * @param vertx the current Vert.x instance
   */
  public ElasticsearchClientFactory(Vertx vertx) {
    this.vertx = vertx;
  }
  
  /**
   * Create an Elasticsearch client. Either start an Elasticsearch instance or
   * connect to an external one - depending on the configuration.
   * @param indexName the name of the index the Elasticsearch client will
   * operate on
   * @return a single emitting an Elasticsearch client and runner
   */
  public Single<ElasticsearchClient> createElasticsearchClient(String indexName) {
    JsonObject config = vertx.getOrCreateContext().config();
    
    if (config.containsKey(ConfigConstants.INDEX_ELASTICSEARCH_HOST)) {
      log.warn("The configuration item `" + ConfigConstants.INDEX_ELASTICSEARCH_HOST
        + "' is deprecated and will be removed in a future release. Please use `"
        + ConfigConstants.INDEX_ELASTICSEARCH_HOSTS + "' instead.");
    }
    if (config.containsKey(ConfigConstants.INDEX_ELASTICSEARCH_PORT)) {
      log.warn("The configuration item `" + ConfigConstants.INDEX_ELASTICSEARCH_PORT
          + "' is deprecated and will be removed in a future release. Please use `"
          + ConfigConstants.INDEX_ELASTICSEARCH_HOSTS + "' instead.");
    }

    JsonArray hosts = config.getJsonArray(ConfigConstants.INDEX_ELASTICSEARCH_HOSTS);
    if (hosts == null || hosts.isEmpty()) {
      String host = config.getString(ConfigConstants.INDEX_ELASTICSEARCH_HOST, "localhost");
      int port = config.getInteger(ConfigConstants.INDEX_ELASTICSEARCH_PORT, 9200);
      log.warn("Configuration item `" + ConfigConstants.INDEX_ELASTICSEARCH_HOSTS
          + "' not set. Using " + host + ":" + port);
      hosts = new JsonArray().add(host + ":" + port);
    }

    List<URI> uris = hosts.stream()
        .map(h -> URI.create("http://" + h))
        .collect(Collectors.toList());

    long autoUpdateHostsIntervalSeconds = config.getLong(
        ConfigConstants.INDEX_ELASTICSEARCH_AUTO_UPDATE_HOSTS_INTERVAL_SECONDS, -1L);
    Duration autoUpdateHostsInterval = null;
    if (autoUpdateHostsIntervalSeconds > 0) {
      autoUpdateHostsInterval = Duration.ofSeconds(autoUpdateHostsIntervalSeconds);
    }

    boolean compressRequestBodies = config.getBoolean(
      ConfigConstants.INDEX_ELASTICSEARCH_COMPRESS_REQUEST_BODIES, false);

    ElasticsearchClient client = new RemoteElasticsearchClient(uris, indexName,
        autoUpdateHostsInterval, compressRequestBodies, vertx);
    
    return Single.just(client);
  }
}
