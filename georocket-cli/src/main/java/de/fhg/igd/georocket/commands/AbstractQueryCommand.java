package de.fhg.igd.georocket.commands;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.URLEncoder;

import io.vertx.core.Handler;
import io.vertx.core.http.HttpClient;

/**
 * Abstract base class for commands that need to export data
 * @author Michel Kraemer
 */
public abstract class AbstractQueryCommand extends AbstractGeoRocketCommand {
  /**
   * Export using a search query and a layer
   * @param query the search query (may be null)
   * @param layer the layer to export (may be null)
   * @param out the writer to write the results to
   * @param handler the handler that should be called when all
   * chunks have been exported
   * @throws IOException if the query or the layer was invalid
   */
  protected void export(String query, String layer, PrintWriter out,
      Handler<Integer> handler) throws IOException {
    if (layer == null || layer.isEmpty()) {
      layer = "/";
    }
    if (!layer.endsWith("/")) {
      layer += "/";
    }
    if (!layer.startsWith("/")) {
      layer = "/" + layer;
    }
    
    String urlQuery = "";
    if (query != null && !query.isEmpty()) {
      urlQuery = "?search=" + URLEncoder.encode(query, "UTF-8");
    }
    
    HttpClient client = vertx.createHttpClient();
    client.getNow(63074, "localhost", "/store" + layer + urlQuery, response -> {
      if (response.statusCode() != 200) {
        error(response.statusMessage());
        client.close();
        handler.handle(1);
      } else {
        response.handler(buf -> {
          out.write(buf.toString());
        });
        response.endHandler(v -> {
          client.close();
          handler.handle(0);
        });
      }
    });
  }
}
