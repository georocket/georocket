package io.georocket.mocks;

import io.georocket.constants.ConfigConstants;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.rx.java.ObservableFuture;
import io.vertx.rx.java.RxHelper;
import rx.Observable;

public class MockServer extends AbstractVerticle {

  
  @Override
  public void start() {
  }

  public static Observable<HttpServer> deployHttpServer(Vertx vertx, JsonObject config, Router router) {
    String host = config.getString(ConfigConstants.HOST, ConfigConstants.DEFAULT_HOST);
    int port = config.getInteger(ConfigConstants.PORT, ConfigConstants.DEFAULT_PORT);

    HttpServerOptions serverOptions = new HttpServerOptions().setCompressionSupported(true);
    HttpServer server = vertx.createHttpServer(serverOptions);

    ObservableFuture<HttpServer> observable = RxHelper.observableFuture();
    server.requestHandler(router::accept).listen(port, host, observable.toHandler());
    return observable;
  }

}
