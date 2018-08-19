package io.georocket.index.elasticsearch;

import io.georocket.constants.ConfigConstants;
import io.vertx.core.impl.NoStackTraceThrowable;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.rxjava.core.Vertx;
import org.apache.commons.io.IOUtils;
import rx.Single;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
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
    
    boolean embedded = config.getBoolean(ConfigConstants.INDEX_ELASTICSEARCH_EMBEDDED, true);

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

    if (embedded && hosts.size() > 1) {
      log.warn("There are more than one Elasticsearch hosts configured in `"
         + ConfigConstants.INDEX_ELASTICSEARCH_HOSTS + "' but embedded mode is "
         + "enabled. Only the first host will be considered.");
      hosts = new JsonArray().add(hosts.getString(0));
    }

    List<URI> uris = hosts.stream()
        .map(h -> URI.create("http://" + h))
        .collect(Collectors.toList());

    long autoUpdateHostsIntervalSeconds = config.getLong(
        ConfigConstants.INDEX_ELASTICSEARCH_AUTO_UPDATE_HOSTS_INTERVAL_SECONDS, -1L);
    Duration autoUpdateHostsInterval = null;
    if (!embedded && autoUpdateHostsIntervalSeconds > 0) {
      autoUpdateHostsInterval = Duration.ofSeconds(autoUpdateHostsIntervalSeconds);
    }

    boolean compressRequestBodies = config.getBoolean(
      ConfigConstants.INDEX_ELASTICSEARCH_COMPRESS_REQUEST_BODIES, false);

    ElasticsearchClient client = new RemoteElasticsearchClient(uris, indexName,
        autoUpdateHostsInterval, compressRequestBodies, vertx.getDelegate());
    
    if (!embedded) {
      // just return the client
      return Single.just(client);
    }
    
    return client.isRunning().flatMap(running -> {
      if (running) {
        // we don't have to start Elasticsearch again
        return Single.just(client);
      }

      String home = config.getString(ConfigConstants.HOME);

      String defaultElasticsearchDownloadUrl;
      try {
        defaultElasticsearchDownloadUrl = IOUtils.toString(getClass().getResource(
                "/elasticsearch_download_url.txt"), StandardCharsets.UTF_8);
      } catch (IOException e) {
        return Single.error(e);
      }

      String elasticsearchDownloadUrl = config.getString(
              ConfigConstants.INDEX_ELASTICSEARCH_DOWNLOAD_URL, defaultElasticsearchDownloadUrl);

      Pattern pattern = Pattern.compile("-([0-9]\\.[0-9]\\.[0-9])\\.zip$");
      Matcher matcher = pattern.matcher(elasticsearchDownloadUrl);
      if (!matcher.find()) {
        return Single.error(new NoStackTraceThrowable("Could not extract "
          + "version number from Elasticsearch download URL: "
          + elasticsearchDownloadUrl));
      }
      String elasticsearchVersion = matcher.group(1);

      String elasticsearchInstallPath = config.getString(
              ConfigConstants.INDEX_ELASTICSEARCH_INSTALL_PATH,
              home + "/elasticsearch/" + elasticsearchVersion);

      String host = uris.get(0).getHost();
      int port = uris.get(0).getPort();

      // install Elasticsearch, start it and then create the client
      ElasticsearchInstaller installer = new ElasticsearchInstaller(vertx);
      ElasticsearchRunner runner = new ElasticsearchRunner(vertx);
      return installer.download(elasticsearchDownloadUrl, elasticsearchInstallPath)
        .flatMapCompletable(path -> runner.runElasticsearch(host, port, path))
        .andThen(runner.waitUntilElasticsearchRunning(client))
        .toSingle(() -> new EmbeddedElasticsearchClient(client, runner));
    });
  }
}
