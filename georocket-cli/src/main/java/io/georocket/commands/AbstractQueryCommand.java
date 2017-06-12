package io.georocket.commands;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.NoSuchElementException;

import io.georocket.client.GeoRocketClient;
import io.vertx.core.Handler;
import io.vertx.core.impl.NoStackTraceThrowable;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

/**
 * Abstract base class for commands that need to export data
 * @author Michel Kraemer
 */
public abstract class AbstractQueryCommand extends AbstractGeoRocketCommand {
  private static Logger log = LoggerFactory.getLogger(AbstractQueryCommand.class);
  
  /**
   * Query data store using a search query and a layer
   * @param query the search query (may be null)
   * @param layer the layer to export (may be null)
   * @param out the writer to write the results to
   * @param handler the handler that should be called when all
   * chunks have been exported
   * @throws IOException if the query or the layer was invalid
   */
  protected void query(String query, String layer, PrintWriter out,
      Handler<Integer> handler) throws IOException {
    GeoRocketClient client = createClient();
    client.getStore().search(query, layer, ar -> {
      if (ar.failed()) {
        error(ar.cause().getMessage());
        if (!(ar.cause() instanceof NoSuchElementException)
            && !(ar.cause() instanceof NoStackTraceThrowable)) {
          log.error("Could not query store", ar.cause());
        }
        handler.handle(1);
      } else {
        ar.result().handler(buf -> {
          out.write(buf.toString("utf-8"));
        });
        ar.result().endHandler(v -> {
          client.close();
          handler.handle(0);
        });
      }
    });
  }
}
