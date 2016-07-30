package io.georocket.storage.mongodb;

import java.io.IOException;

import com.mongodb.ServerAddress;

import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.MongodProcess;
import de.flapdoodle.embed.mongo.MongodStarter;
import de.flapdoodle.embed.mongo.config.MongodConfigBuilder;
import de.flapdoodle.embed.mongo.config.Net;
import de.flapdoodle.embed.mongo.distribution.Version;
import de.flapdoodle.embed.process.runtime.Network;
import io.georocket.NetUtils;

/**
 * Starts and stops a MongoDB instance
 * @author Michel Kraemer
 */
public class MongoDBTestConnector {
  private static final MongodStarter starter = MongodStarter.getDefaultInstance();
  
  private final MongodExecutable mongodExe;
  private final MongodProcess mongod;
  
  /**
   * The default name of the database to test against
   */
  public static String MONGODB_DBNAME = "testdb";
  
  /**
   * The address of the MongoDB instance
   */
  public final ServerAddress serverAddress =
      new ServerAddress("localhost", NetUtils.findPort());
  
  /**
   * Start MongoDB instance. Don't forget to call {@link #stop()}
   * if you don't need it anymore!
   * @throws IOException if the instance could not be started
   */
  public MongoDBTestConnector() throws IOException {
    mongodExe = starter.prepare(new MongodConfigBuilder()
        .version(Version.Main.PRODUCTION)
        .net(new Net(serverAddress.getPort(), Network.localhostIsIPv6()))
        .build());
    mongod = mongodExe.start();
  }
  
  /**
   * Stop MongoDB instance
   */
  public void stop() {
    mongod.stop();
    mongodExe.stop();
  }
}
