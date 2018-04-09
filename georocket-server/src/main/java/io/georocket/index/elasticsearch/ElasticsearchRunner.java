package io.georocket.index.elasticsearch;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecuteResultHandler;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteException;
import org.apache.commons.exec.ExecuteWatchdog;
import org.apache.commons.exec.Executor;
import org.apache.commons.exec.ShutdownHookProcessDestroyer;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.SystemUtils;

import io.georocket.constants.ConfigConstants;
import io.georocket.util.RxUtils;
import io.vertx.core.impl.NoStackTraceThrowable;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.rxjava.core.Vertx;
import rx.Completable;
import rx.Observable;
import rx.Single;

/**
 * Runs an Elasticsearch instance
 * @author Michel Kraemer
 */
public class ElasticsearchRunner {
  private static Logger log = LoggerFactory.getLogger(ElasticsearchRunner.class);
  
  private final Vertx vertx;
  private Executor executor;
  private boolean stopped;
  
  /**
   * Create the Elasticsearch runner
   * @param vertx the Vert.x instance
   */
  public ElasticsearchRunner(Vertx vertx) {
    this.vertx = vertx;
  }
  
  /**
   * Run Elasticsearch
   * @param host the host Elasticsearch should bind to
   * @param port the port Elasticsearch should listen on
   * @param elasticsearchInstallPath the path where Elasticsearch is installed
   * @return a Completable that completes when Elasticsearch has started
   */
  public Completable runElasticsearch(String host, int port,
      String elasticsearchInstallPath) {
    JsonObject config = vertx.getOrCreateContext().config();
    String storage = config.getString(ConfigConstants.STORAGE_FILE_PATH);
    String root = storage + "/index";
    
    return vertx.<Void>rxExecuteBlocking(f -> {
      log.info("Starting Elasticsearch ...");
      
      // get Elasticsearch executable
      String executable = FilenameUtils.separatorsToSystem(
          elasticsearchInstallPath);
      executable = FilenameUtils.concat(executable, "bin");
      if (SystemUtils.IS_OS_WINDOWS) {
        executable = FilenameUtils.concat(executable, "elasticsearch.bat");
      } else {
        executable = FilenameUtils.concat(executable, "elasticsearch");
      }
      
      // start Elasticsearch
      CommandLine cmdl = new CommandLine(executable);
      cmdl.addArgument("-Ecluster.name=georocket-cluster");
      cmdl.addArgument("-Enode.name=georocket-node");
      cmdl.addArgument("-Enetwork.host=" + host);
      cmdl.addArgument("-Ehttp.port=" + port);
      cmdl.addArgument("-Epath.data=" + root + "/data");
      
      executor = new DefaultExecutor();
      executor.setProcessDestroyer(new ShutdownHookProcessDestroyer());
      executor.setWatchdog(new ExecuteWatchdog(ExecuteWatchdog.INFINITE_TIMEOUT));
      
      // set ES_JAVA_OPTS environment variable if necessary
      Map<String, String> env = new HashMap<>(System.getenv());
      if (!env.containsKey("ES_JAVA_OPTS")) {
        String javaOpts = config.getString(ConfigConstants.INDEX_ELASTICSEARCH_JAVA_OPTS);
        if (javaOpts != null) {
          env.put("ES_JAVA_OPTS", javaOpts);
        }
      }
      
      try {
        executor.execute(cmdl, env, new DefaultExecuteResultHandler() {
          @Override
          public void onProcessComplete(final int exitValue) {
            log.info("Elasticsearch quit with exit code: " + exitValue);
          }
          
          @Override
          public void onProcessFailed(final ExecuteException e) {
            if (!stopped) {
              log.error("Elasticsearch execution failed", e);
            }
          }
        });
        f.complete();
      } catch (IOException e) {
        f.fail(e);
      }
    }).toCompletable();
  }
  
  /**
   * Wait 60 seconds or until Elasticsearch is up and running, whatever
   * comes first
   * @param client the client to use to check if Elasticsearch is running
   * @return a Completable that will complete when Elasticsearch is running
   */
  public Completable waitUntilElasticsearchRunning(
      ElasticsearchClient client) {
    final Throwable repeat = new NoStackTraceThrowable("");
    return Single.defer(client::isRunning).flatMap(running -> {
      if (!running) {
        return Single.error(repeat);
      }
      return Single.just(running);
    }).retryWhen(errors -> {
      Observable<Throwable> o = errors.flatMap(t -> {
        if (t == repeat) {
          // Elasticsearch is still not up, retry
          return Observable.just(t);
        }
        // forward error
        return Observable.error(t);
      });
      // retry for 60 seconds
      return RxUtils.makeRetry(60, 1000, null).call(o);
    }).toCompletable();
  }
  
  /**
   * Stop a running Elasticsearch instance
   */
  public void stop() {
    if (executor != null && !stopped) {
      stopped = true;
      executor.getWatchdog().destroyProcess();
    }
  }
}
