package io.georocket;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.yaml.snakeyaml.Yaml;

import io.georocket.constants.ConfigConstants;
import io.georocket.http.Endpoint;
import io.georocket.http.GeneralEndpoint;
import io.georocket.http.StoreEndpoint;
import io.georocket.index.ChunkIndexerVerticle;
import io.georocket.util.JsonUtils;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.Verticle;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.Router;
import io.vertx.rx.java.ObservableFuture;
import io.vertx.rx.java.RxHelper;
import rx.Observable;
import rx.plugins.RxJavaHooks;

/**
 * GeoRocket - A high-performance database for geospatial files
 * @author Michel Kraemer
 */
public class GeoRocket extends AbstractVerticle {
  private static Logger log = LoggerFactory.getLogger(GeoRocket.class);
  
  protected static File geoRocketHome;

  /**
   * Deploy a new verticle with the standard configuration of this instance
   * @param cls the class of the verticle class to deploy
   * @return an observable that will carry the verticle's deployment id
   */
  protected Observable<String> deployVerticle(Class<? extends Verticle> cls) {
    ObservableFuture<String> observable = RxHelper.observableFuture();
    DeploymentOptions options = new DeploymentOptions().setConfig(config());
    vertx.deployVerticle(cls.getName(), options, observable.toHandler());
    return observable;
  }

  /**
   * Deploy the chunk indexer verticle
   * @return an observable that will complete when the verticle was deployed
   * and will carry the verticle's deployment id
   */
  protected Observable<String> deployChunkIndexer() {
    return deployVerticle(ChunkIndexerVerticle.class);
  }

  /**
   * Deploy the importer verticle
   * @return an observable that will complete when the verticle was deployed
   * and will carry the verticle's deployment id
   */
  protected Observable<String> deployImporter() {
    return deployVerticle(ImporterVerticle.class);
  }

  private Observable<HttpServer> deployHttpServer() {
    int port = config().getInteger(ConfigConstants.PORT, ConfigConstants.DEFAULT_PORT);

    Router router = createRouter();
    HttpServerOptions serverOptions = createHttpServerOptions();
    HttpServer server = vertx.createHttpServer(serverOptions);

    ObservableFuture<HttpServer> observable = RxHelper.observableFuture();
    server.requestHandler(router::accept).listen(port, observable.toHandler());
    return observable;
  }
  
  /**
   * Creates the HTTP endpoint handling requests related to the data store.
   * Returns {@link StoreEndpoint} by default. Subclasses may override if
   * they want to return another implementation.
   * @return the endpoint
   */
  protected Endpoint createStoreEndpoint() {
    return new StoreEndpoint(vertx);
  }
  
  /**
   * Creates the HTTP endpoint handling general requests
   * Returns {@link GeneralEndpoint} by default. Subclasses may override if
   * they want to return another implementation.
   * @return the endpoint
   */
  protected Endpoint createGeneralEndpoint() {
    return new GeneralEndpoint(vertx);
  }

  /**
   * Create a {@link Router} and add routes for <code>/store/</code>
   * to it. Sub-classes may override if they want to add further routes
   * @return the created {@link Router}
   */
  protected Router createRouter() {
    Router router = Router.router(vertx);
    
    Endpoint storeEndpoint = createStoreEndpoint();
    router.mountSubRouter("/store", storeEndpoint.createRouter());
    
    Endpoint generalEndpoint = createGeneralEndpoint();
    router.mountSubRouter("/", generalEndpoint.createRouter());
    
    return router;
  }

  /**
   * Create a {@link HttpServerOptions} and set <code>Compression
   * Support</code> as option. Sub-classes may override if they want to
   * add further options
   * @return the created {@link HttpServerOptions}
   */
  protected HttpServerOptions createHttpServerOptions() {
    HttpServerOptions serverOptions = new HttpServerOptions()
        .setCompressionSupported(true);
    return serverOptions;
  }
  
  @Override
  public void start(Future<Void> startFuture) {
    log.info("Launching GeoRocket ...");

    deployChunkIndexer()
      .flatMap(v -> deployImporter())
      .flatMap(v -> deployHttpServer())
      .subscribe(id -> {
        log.info("GeoRocket launched successfully.");
        startFuture.complete();
      }, startFuture::fail);
  }
  
  /**
   * Replace configuration variables in a string
   * @param str the string
   * @return a copy of the given string with configuration variables replaced
   */
  private static String replaceConfVariables(String str) {
    return str.replace("$GEOROCKET_HOME", geoRocketHome.getAbsolutePath());
  }
  
  /**
   * Recursively replace configuration variables in an array
   * @param arr the array
   * @return a copy of the given array with configuration variables replaced
   */
  private static JsonArray replaceConfVariables(JsonArray arr) {
    JsonArray result = new JsonArray();
    for (Object o : arr) {
      if (o instanceof JsonObject) {
        replaceConfVariables((JsonObject)o);
      } else if (o instanceof JsonArray) {
        o = replaceConfVariables((JsonArray)o);
      } else if (o instanceof String) {
        o = replaceConfVariables((String)o);
      }
      result.add(o);
    }
    return result;
  }
  
  /**
   * Recursively replace configuration variables in an object
   * @param obj the object
   */
  private static void replaceConfVariables(JsonObject obj) {
    Set<String> keys = new HashSet<>(obj.getMap().keySet());
    for (String key : keys) {
      Object value = obj.getValue(key);
      if (value instanceof JsonObject) {
        replaceConfVariables((JsonObject)value);
      } else if (value instanceof JsonArray) {
        JsonArray arr = replaceConfVariables((JsonArray)value);
        obj.put(key, arr);
      } else if (value instanceof String) {
        String newValue = replaceConfVariables((String)value);
        obj.put(key, newValue);
      }
    }
  }
  
  /**
   * Set default configuration values
   * @param conf the current configuration
   */
  private static void setDefaultConf(JsonObject conf) {
    conf.put(ConfigConstants.HOME, "$GEOROCKET_HOME");
    if (!conf.containsKey(ConfigConstants.STORAGE_FILE_PATH)) {
      conf.put(ConfigConstants.STORAGE_FILE_PATH, "$GEOROCKET_HOME/storage");
    }
  }

  /**
   * Load the GeoRocket configuration
   * @return the configuration
   *
   * @throws IOException If the georocket home is invalid or the file could not be accessed
   * @throws DecodeException If the configuration could not be decoded from json
   */
  protected static JsonObject loadGeoRocketConfiguration() throws IOException, DecodeException {
    String geoRocketHomeStr = System.getenv("GEOROCKET_HOME");
    if (geoRocketHomeStr == null) {
      log.info("Environment variable GEOROCKET_HOME not set. Using current "
          + "working directory.");
      geoRocketHomeStr = new File(".").getAbsolutePath();
    }

    geoRocketHome = new File(geoRocketHomeStr).getCanonicalFile();

    log.info("Using GeoRocket home " + geoRocketHome);

    // load configuration file
    File confDir = new File(geoRocketHome, "conf");
    File confFile = new File(confDir, "georocketd.yaml");
    if (!confFile.exists()) {
      confFile = new File(confDir, "georocketd.yml");
      if (!confFile.exists()) {
        confFile = new File(confDir, "georocketd.json");
      }
    }
    String confFileStr = FileUtils.readFileToString(confFile, "UTF-8");
    JsonObject conf;
    if (confFile.getName().endsWith(".json")) {
      conf = new JsonObject(confFileStr);
    } else {
      Yaml yaml = new Yaml();
      @SuppressWarnings("unchecked")
      Map<String, Object> m = yaml.loadAs(confFileStr, Map.class);
      conf = JsonUtils.flatten(new JsonObject(m));
    }

    // set default configuration values
    setDefaultConf(conf);

    // replace variables in config
    replaceConfVariables(conf);

    return conf;
  }

  /**
   * Runs the server
   * @param args the command line arguments
   */
  public static void main(String[] args) {
    Vertx vertx = Vertx.vertx();

    // register schedulers that run Rx operations on the Vert.x event bus
    RxJavaHooks.setOnComputationScheduler(s -> RxHelper.scheduler(vertx));
    RxJavaHooks.setOnIOScheduler(s -> RxHelper.blockingScheduler(vertx));
    RxJavaHooks.setOnNewThreadScheduler(s -> RxHelper.scheduler(vertx));

    DeploymentOptions options = new DeploymentOptions();

    try {
      JsonObject conf = loadGeoRocketConfiguration();
      options.setConfig(conf);
    } catch (IOException ex) {
      log.fatal("Invalid georocket home", ex);
      System.exit(1);
    } catch (DecodeException ex) {
      log.fatal("Failed to decode the GeoRocket (JSON) configuration", ex);
      System.exit(1);
    }

    // deploy main verticle
    vertx.deployVerticle(GeoRocket.class.getName(), options, ar -> {
        if (ar.failed()) {
          log.fatal("Could not deploy GeoRocket");
          ar.cause().printStackTrace();
          System.exit(1);
        }
      });
  }
}
