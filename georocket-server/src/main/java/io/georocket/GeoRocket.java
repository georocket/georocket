package io.georocket;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import io.georocket.http.Endpoint;
import io.georocket.http.GeneralEndpoint;
import io.georocket.http.StoreEndpoint;
import io.georocket.index.MetadataVerticle;
import io.vertx.core.net.PemKeyCertOptions;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.BooleanUtils;
import org.yaml.snakeyaml.Yaml;

import io.georocket.constants.ConfigConstants;
import io.georocket.index.IndexerVerticle;
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
import rx.Single;
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
   * @return a single that will carry the verticle's deployment id
   */
  protected Single<String> deployVerticle(Class<? extends Verticle> cls) {
    ObservableFuture<String> observable = RxHelper.observableFuture();
    DeploymentOptions options = new DeploymentOptions().setConfig(config());
    vertx.deployVerticle(cls.getName(), options, observable.toHandler());
    return observable.toSingle();
  }

  /**
   * Deploy the indexer verticle
   * @return a single that will complete when the verticle was deployed
   * and will carry the verticle's deployment id
   */
  protected Single<String> deployIndexer() {
    return deployVerticle(IndexerVerticle.class);
  }

  /**
   * Deploy the importer verticle
   * @return a single that will complete when the verticle was deployed
   * and will carry the verticle's deployment id
   */
  protected Single<String> deployImporter() {
    return deployVerticle(ImporterVerticle.class);
  }

  /**
   * Deploy the metadata verticle
   * @return a single that will complete when the verticle was deployed
   * and will carry the verticle's deployment id
   */
  protected Single<String> deployMetadata() {
    return deployVerticle(MetadataVerticle.class);
  }

  /**
   * Deploy the http server.
   * @return a single that will complete when the http server was started.
   */
  protected Single<HttpServer> deployHttpServer() {
    String host = config().getString(ConfigConstants.HOST, ConfigConstants.DEFAULT_HOST);
    int port = config().getInteger(ConfigConstants.PORT, ConfigConstants.DEFAULT_PORT);

    Router router = createRouter();
    HttpServerOptions serverOptions = createHttpServerOptions();
    try {
      HttpServer server = vertx.createHttpServer(serverOptions);
      ObservableFuture<HttpServer> observable = RxHelper.observableFuture();
      server.requestHandler(router::accept).listen(port, host, observable.toHandler());
      return observable.toSingle();
    } catch (Throwable t) {
      return Single.error(t);
    }
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

    router.route().handler(ctx -> {
      String reason = "The endpoint " + ctx.request().path() + " does not exist";
      ctx.response()
        .setStatusCode(404)
        .end(ServerAPIException.toJson("endpoint_not_found", reason).toString());
    });
    
    return router;
  }

  /**
   * Create an {@link HttpServerOptions} object and modify it according to the
   * configuration. Sub-classes may override this method to further modify the
   * object.
   * @return the created {@link HttpServerOptions}
   */
  protected HttpServerOptions createHttpServerOptions() {
    boolean compress = config().getBoolean(ConfigConstants.HTTP_COMPRESS, true);

    HttpServerOptions serverOptions = new HttpServerOptions()
        .setCompressionSupported(compress);

    boolean ssl = config().getBoolean(ConfigConstants.HTTP_SSL, false);
    if (ssl) {
      serverOptions.setSsl(ssl);
      String certPath = config().getString(ConfigConstants.HTTP_CERT_PATH, null);
      String keyPath = config().getString(ConfigConstants.HTTP_KEY_PATH, null);
      PemKeyCertOptions pemKeyCertOptions = new PemKeyCertOptions()
          .setCertPath(certPath)
          .setKeyPath(keyPath);
      serverOptions.setPemKeyCertOptions(pemKeyCertOptions);
    }

    boolean alpn = config().getBoolean(ConfigConstants.HTTP_ALPN, false);
    if (alpn) {
      if (!ssl) {
        log.warn("ALPN is enabled but SSL is not! In order for ALPN to work " +
            "correctly, SSL is required.");
      }
      serverOptions.setUseAlpn(alpn);
    }

    return serverOptions;
  }
  
  @Override
  public void start(Future<Void> startFuture) {
    log.info("Launching GeoRocket ...");

    deployIndexer()
      .flatMap(v -> deployImporter())
      .flatMap(v -> deployMetadata())
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
    
    overwriteWithEnvironmentVariables(conf);

    return conf;
  }

  /**
   * Match every environment variable against the config keys from
   * {{@link ConfigConstants#getConfigKeys()}} and save the found values using
   * the config key in the config object. The method is equivalent to calling
   * <code>overwriteWithEnvironmentVariables(conf, java.lang.System.getenv())</code>
   * @param conf the config object
   */
  private static void overwriteWithEnvironmentVariables(JsonObject conf) {
    overwriteWithEnvironmentVariables(conf, System.getenv());
  }

  /**
   * Match every environment variable against the config keys from
   * {{@link ConfigConstants#getConfigKeys()}} and save the found values using
   * the config key in the config object.
   * @param conf the config object
   * @param env the map with the environment variables
   */
  static void overwriteWithEnvironmentVariables(JsonObject conf,
    Map<String, String> env) {
    Map<String, String> names = ConfigConstants.getConfigKeys()
      .stream()
      .collect(Collectors.toMap(
        s -> s.toUpperCase().replace(".", "_"),
        Function.identity()
      ));
    env.forEach((key, val) -> {
      String name = names.get(key.toUpperCase());
      if (name != null) {
        Object newVal = toJsonType(val);
        conf.put(name, newVal);
      }
    });
  }

  /**
   * Parse a string into int, double, boolean or keep it as String.
   * @param val The string to parse
   * @return The parsed value as object.
   */
  private static Object toJsonType(String val) {
    try {
      return Integer.parseInt(val);
    } catch (NumberFormatException ex) {
      // ok
    }
    try {
      return Double.parseDouble(val);
    } catch (NumberFormatException ex) {
      // ok
    }
    Boolean bool = BooleanUtils.toBooleanObject(val);
    if (bool != null) {
      return bool;
    }
    return val;    
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
    
    boolean logConfig = options.getConfig().getBoolean(
        ConfigConstants.LOG_CONFIG, false);
    if (logConfig) {
      log.info("Configuration:\n" + options.getConfig().encodePrettily());
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
