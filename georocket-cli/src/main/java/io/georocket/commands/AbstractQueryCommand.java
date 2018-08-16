package io.georocket.commands;

import io.georocket.client.GeoRocketClient;
import io.georocket.client.SearchParams;
import io.georocket.client.SearchReadStream;
import io.georocket.client.SearchResult;
import io.vertx.core.Handler;
import io.vertx.core.impl.NoStackTraceThrowable;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.NoSuchElementException;

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
    query(new SearchParams().setQuery(query).setLayer(layer), out, handler);
  }

  /**
   * Query data store using a search query and a layer
   * @param params search parameters
   * @param out the writer to write the results to
   * @param handler the handler that should be called when all
   * chunks have been exported
   * @throws IOException if the query or the layer was invalid
   */
  protected void query(SearchParams params, PrintWriter out,
      Handler<Integer> handler) throws IOException {
    GeoRocketClient client = createClient();
    client.getStore().search(params, ar -> {
      if (ar.failed()) {
        error(ar.cause().getMessage());
        if (!(ar.cause() instanceof NoSuchElementException)
            && !(ar.cause() instanceof NoStackTraceThrowable)) {
          log.error("Could not query store", ar.cause());
        }
        handler.handle(1);
      } else {
        SearchResult sr = ar.result();
        SearchReadStream r = sr.getResponse();
        r.handler(buf -> out.write(buf.toString("utf-8")));
        r.endHandlerWithResult(srsr -> {
          if (srsr.getUnmergedChunks() > 0) {
            System.err.println(srsr.getUnmergedChunks() + " chunks could not " +
              "be merged. This usually has one of the following causes:\n" +
              "\n" +
              "* Chunks were added to GeoRocket's store while merging was " +
              "in progress\n" +
              "* Optimistic merging was enabled and some chunks did not fit " +
              "to the search\n" +
              "  result\n");
            if (params.isOptimisticMerging()) {
              System.err.println("Optimistic merging was enabled. Repeat the " +
                "request with optimistic merging\n" +
                "disabled if you want to get all chunks.");
            } else {
              System.err.println("Repeat the request if you want to get all chunks.");
            }
          }

          client.close();
          handler.handle(0);
        });
      }
    });
  }
}
