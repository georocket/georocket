package io.georocket.migration;

import com.github.zafarkhaja.semver.Version;
import io.vertx.core.Vertx;
import rx.Single;

/**
 * Script to migrate data from one version to another.
 * @author Andrej Sajenko
 */
public interface MigrationScript extends Comparable<MigrationScript> {

  /**
   * Run migration code on vertx context.
   * @param migrationManager the migration manager.
   * @param vertx vertx context.
   * @return Single which emmit when migration was done.
   */
  Single<Void> migrate(MigrationManager migrationManager, Vertx vertx);

  /**
   * @return The target version for this migration script.
   */
  Version getTargetVersion();

  /**
   * @return The description of the migration task.
   */
  String getMigrationDescription();

  @Override
  default int compareTo(MigrationScript other) {
    return this.getTargetVersion().compareTo(other.getTargetVersion());
  }
}
