package io.georocket.migration.scripts;

import com.github.zafarkhaja.semver.Version;
import io.georocket.constants.ConfigConstants;
import io.georocket.index.elasticsearch.ElasticsearchClientFactory;
import io.georocket.migration.MigrationManager;
import io.georocket.migration.MigrationScript;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import rx.Single;

/**
 * Take the georocket index and reindex it with the same mappings and settings
 * under a new name. The new name contains the version of georocket as suffix
 * and has a alias "georocket".
 * @author Andrej Sajenko
 */
public class V1_1_0_IntroduceIndexAlias implements MigrationScript {

  /**
   * Required default constructor.
   */
  public V1_1_0_IntroduceIndexAlias() {
  }

  @Override
  public Single<Void> migrate(MigrationManager migrationManager, Vertx vertx) {
    ElasticsearchClientFactory esFactory = new ElasticsearchClientFactory(new io.vertx.rxjava.core.Vertx(vertx));

    Version target = getTargetVersion();
    String version = target.getNormalVersion();
    String newIndex = ConfigConstants.ES_INDEX_NAME + "_" + version;

    return migrationManager.ensureNewIndex(newIndex)
      .flatMap(v -> esFactory.createElasticsearchClient(ConfigConstants.ES_INDEX_NAME))
      .flatMap(client -> client.reindex(
          new JsonObject().put("index", ConfigConstants.ES_INDEX_NAME),
          new JsonObject().put("index", newIndex)
        )
        .flatMap(v -> client.addAlias(ConfigConstants.ES_INDEX_NAME, newIndex))
        .flatMap(v -> client.delete())
        .flatMap(v -> migrationManager.moveAlias())
      );
  }

  @Override
  public Version getTargetVersion() {
    return Version.valueOf("1.1.0");
  }

  @Override
  public String getMigrationDescription() {
    return "Introduce index alias for georocket elasticsearch index.";
  }
}
