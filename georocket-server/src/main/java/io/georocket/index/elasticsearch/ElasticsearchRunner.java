package io.georocket.index.elasticsearch;

import java.io.IOException;

import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecuteResultHandler;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteException;
import org.apache.commons.exec.Executor;
import org.apache.commons.exec.ShutdownHookProcessDestroyer;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.SystemUtils;

import io.georocket.constants.ConfigConstants;
import io.georocket.util.RxUtils;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.impl.NoStackTraceThrowable;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.rx.java.ObservableFuture;
import io.vertx.rx.java.RxHelper;
import rx.Observable;
import rx.Scheduler;

/**
 * Runs an Elasticsearch instance
 * @author Michel Kraemer
 */
public class ElasticsearchRunner {
  private static Logger log = LoggerFactory.getLogger(ElasticsearchRunner.class);
  
  private final Vertx vertx;
  private Executor executor;
  
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
   * @return an observable that emits exactly one item when Elasticsearch has started
   */
  public Observable<Void> runElasticsearch(String host, int port,
      String elasticsearchInstallPath) {
    JsonObject config = vertx.getOrCreateContext().config();
    String storage = config.getString(ConfigConstants.STORAGE_FILE_PATH);
    String root = storage + "/index";
    
    ObservableFuture<Void> observable = RxHelper.observableFuture();
    Handler<AsyncResult<Void>> handler = observable.toHandler();
    
    vertx.<Void>executeBlocking(f -> {
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
      cmdl.addArgument("--cluster.name");
      cmdl.addArgument("georocket-cluster");
      cmdl.addArgument("--node.name");
      cmdl.addArgument("georocket-node");
      cmdl.addArgument("--network.host");
      cmdl.addArgument(host);
      cmdl.addArgument("--http.port");
      cmdl.addArgument(String.valueOf(port));
      System.err.println("POERR: " + port);
      cmdl.addArgument("--path.home");
      cmdl.addArgument(elasticsearchInstallPath);
      cmdl.addArgument("--path.data");
      cmdl.addArgument(root + "/data");
      
      executor = new DefaultExecutor();
      executor.setProcessDestroyer(new ShutdownHookProcessDestroyer());
      
      try {
        executor.execute(cmdl, new DefaultExecuteResultHandler() {
          @Override
          public void onProcessComplete(final int exitValue) {
            log.info("Elasticsearch quit with exit code: " + exitValue);
          }
          
          @Override
          public void onProcessFailed(final ExecuteException e) {
            log.error("Elasticsearch execution failed", e);
          }
        });
        f.complete();
      } catch (IOException e) {
        f.fail(e);
      }
    }, handler);
    
    return observable;
  }
  
  /**
   * Wait 60 seconds or until Elasticsearch is up and running, whatever
   * comes first
   * @param client the client to use to check if Elasticsearch is running
   * @return an observable that emits exactly one item when Elasticsearch
   * is running
   */
  public Observable<Void> waitUntilElasticsearchRunning(
      ElasticsearchClient client) {
    Scheduler scheduler = RxHelper.scheduler(vertx);
    final Throwable repeat = new NoStackTraceThrowable("");
    return Observable.<Boolean>create(subscriber -> {
      client.isRunning().subscribe(subscriber);
    }).flatMap(running -> {
      if (!running) {
        return Observable.error(repeat);
      }
      return Observable.just(running);
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
      return RxUtils.makeRetry(60, 1000, scheduler, null).call(o);
    }, scheduler).map(r -> null);
  }
}
