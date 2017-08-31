package io.georocket.migration;

import com.github.zafarkhaja.semver.Version;
import com.google.common.collect.ImmutableList;
import io.georocket.GeoRocket;
import io.georocket.constants.ConfigConstants;
import io.georocket.index.IndexerFactory;
import io.georocket.index.elasticsearch.ElasticsearchClient;
import io.georocket.index.elasticsearch.ElasticsearchClientFactory;
import io.georocket.index.generic.DefaultMetaIndexerFactory;
import io.georocket.util.MapUtils;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.rmi.activation.ActivationGroup_Stub;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import javafx.util.Pair;
import org.apache.commons.io.IOUtils;
import org.jooq.lambda.tuple.Tuple;
import org.jooq.lambda.tuple.Tuple2;
import rx.Observable;
import rx.Single;

/**
 * Handle migration scripts and provide version information.
 * @author Andrej Sajenko
 */
public class MigrationManager {
  private static Logger log = LoggerFactory.getLogger(MigrationManager.class);

  /**
   * Running vertx instance.
   */
  private Vertx vertx;

  /**
   * The current System version of the running application.
   * <b>Note: This is not the version of the data.</b>
   */
  private Version binariesVersion;

  /**
   * Factory to create a elasticsearch client instance.
   */
  private ElasticsearchClientFactory esFactory;

  private MigrationManager(Vertx vertx, Version binariesVersion) {
    this.vertx = vertx;
    this.binariesVersion = binariesVersion;
    this.esFactory = new ElasticsearchClientFactory(new io.vertx.rxjava.core.Vertx(vertx));
  }

  /**
   * @param vertx A running vertx instance.
   * @return Single which emit a MigrationManager instance.
   */
  public static Single<MigrationManager> create(Vertx vertx) {
    return new io.vertx.rxjava.core.Vertx(vertx).<Version>rxExecuteBlocking(handler -> {
      URL u = GeoRocket.class.getResource("version.dat");
      try {
        String version = IOUtils.toString(u, StandardCharsets.UTF_8);
        handler.complete(Version.valueOf(version));
      } catch (IOException e) {
        handler.fail(e);
      }
    }).map(version -> new MigrationManager(vertx, version));
  }

  /**
   * @return The version of the saved data. If no data exist the Single will
   * carry the result of {@link #getBinaryVersion()}.
   */
  public Single<Version> getDataVersion() {
    return this.esFactory
      .createElasticsearchClient(ConfigConstants.ES_INDEX_NAME)
      .flatMap(ElasticsearchClient::indices)
      .flatMap(indicesList -> {
        if (indicesList.isEmpty()) {
          // empty elasticsearch instance -> create
          return Single.just(getBinaryVersion());
        }
        if (indicesList.contains(ConfigConstants.ES_INDEX_NAME)) {
          // georocket is an index -> all migration scripts required
          return Single.just(Version.valueOf("0.0.0"));
        }
        // can read version out of indices name
        return Observable.from(indicesList)
          .filter((index) -> index.startsWith(ConfigConstants.ES_INDEX_NAME + "_"))
          .map((index) -> index.replace(ConfigConstants.ES_INDEX_NAME + "_", ""))
          .filter((index) -> !index.trim().isEmpty())
          .map(Version::valueOf)
          .sorted()
          .last()
          .defaultIfEmpty(getBinaryVersion())
          .toSingle();
      });
  }

  /**
   * Load all migration scripts and run the all scripts which target version is
   * greater than the current data version.
   * @return Single which emit when the migration was finished, can be empty of no migration scripts where
   * executed
   */
  public Observable<Void> migrate() {
    return getDataVersion()
      .flatMapObservable(dataVersion -> {
        if (dataVersion.equals(getBinaryVersion())) {
          return Observable.empty();
        }
        return Observable.from(ImmutableList.copyOf(ServiceLoader.load(MigrationScript.class)))
          .filter(version -> version.getTargetVersion().compareTo(dataVersion) > 0)
          .sorted();
      })
      .flatMapSingle(m -> {
        log.info(String.format("Running migration script [%s]: %s",
          m.getTargetVersion().toString(),
          m.getMigrationDescription()
        ));
        return m.migrate(this, vertx);
      });
  }

  /**
   * Move the elasticsearch alias to the index with the newest version suffix.
   * @return Single emitting when complete.
   */
  public Single<Void> moveAlias() {
    return esFactory
      .createElasticsearchClient(ConfigConstants.ES_INDEX_NAME)
      .flatMap(elasticsearchClient -> elasticsearchClient
        .getAliases()
        .map(aliases -> {
          List<Tuple2<String, String>> aliasesToRemove = new ArrayList<>();
          for (String index: aliases.fieldNames()) {
            JsonObject aliasObject = aliases.getJsonObject(index, new JsonObject())
              .getJsonObject("aliases", new JsonObject());

            for (String alias: aliasObject.fieldNames()) {
              if (alias.equalsIgnoreCase(ConfigConstants.ES_INDEX_NAME)) {
                aliasesToRemove.add(Tuple.tuple(alias, index));
              }
            }
          }
          return aliasesToRemove;
        })
        .flatMapObservable(Observable::from)
        .flatMapSingle(aliasIndex -> elasticsearchClient.removeAlias(aliasIndex.v1, aliasIndex.v2))
        .defaultIfEmpty(null)
        .last()
        .toSingle()
        .flatMap(v -> elasticsearchClient.indices())
        .flatMapObservable(Observable::from)
        .filter((index) -> index.startsWith(ConfigConstants.ES_INDEX_NAME + "_"))
        .map((index) -> index.replace(ConfigConstants.ES_INDEX_NAME + "_", ""))
        .filter((index) -> !index.trim().isEmpty())
        .map(Version::valueOf)
        .sorted()
        .last()
        .toSingle()
        .flatMap(version -> elasticsearchClient
          .addAlias(
            ConfigConstants.ES_INDEX_NAME,
            ConfigConstants.ES_INDEX_NAME + "_" + version.getNormalVersion()
          )
        )
      );
  }

  /**
   * Ensure a index with mappings with the given name.
   * @param indexName The name of the new index
   * @return Single which emits when the operation was finished.
   */
  public Single<Void> ensureNewIndex(String indexName) {
    return esFactory.createElasticsearchClient(indexName)
      .flatMap(client -> {
        List<? extends IndexerFactory> indexerFactories = ImmutableList.copyOf(ServiceLoader.load(IndexerFactory.class));
        Map<String, Object> mappings = new HashMap<>();
        indexerFactories.stream().filter(f -> f instanceof DefaultMetaIndexerFactory)
          .forEach(factory -> MapUtils.deepMerge(mappings, factory.getMapping()));
        indexerFactories.stream().filter(f -> !(f instanceof DefaultMetaIndexerFactory))
          .forEach(factory -> MapUtils.deepMerge(mappings, factory.getMapping()));

        return client.ensureIndex()
          .flatMap(v1 -> client.ensureMapping(ConfigConstants.ES_TYPE_NAME, new JsonObject(mappings)));
      });
  }

  /**
   * @return The version of the running application (GeoRocket binary version)
   */
  public Version getBinaryVersion() {
    return binariesVersion;
  }

}
