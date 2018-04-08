package io.georocket.index.elasticsearch;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.IOUtils;

import io.georocket.constants.ConfigConstants;
import io.vertx.core.impl.NoStackTraceThrowable;
import io.vertx.core.json.JsonObject;
import io.vertx.rxjava.core.Vertx;
import rx.Single;

/**
 * A factory for {@link ElasticsearchClient} instances. Either starts an
 * Elasticsearch instance or connects to an external one - depending on the
 * configuration.
 * @author Michel Kraemer
 */
public class ElasticsearchClientFactory {
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
    String host = config.getString(ConfigConstants.INDEX_ELASTICSEARCH_HOST, "localhost");
    int port = config.getInteger(ConfigConstants.INDEX_ELASTICSEARCH_PORT, 9200);

    ElasticsearchClient client = new RemoteElasticsearchClient(host, port, indexName, vertx);
    
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

      // install Elasticsearch, start it and then create the client
      ElasticsearchInstaller installer = new ElasticsearchInstaller(vertx);
      ElasticsearchRunner runner = new ElasticsearchRunner(vertx);
      return installer.download(elasticsearchDownloadUrl, elasticsearchInstallPath)
        .flatMap(path -> runner.runElasticsearch(host, port, path))
        .flatMap(v -> runner.waitUntilElasticsearchRunning(client))
        .map(v -> new EmbeddedElasticsearchClient(client, runner));
    });
  }
}
