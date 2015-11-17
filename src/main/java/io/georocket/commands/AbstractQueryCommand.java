package io.georocket.commands;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.stream.Collectors;

import com.google.common.base.Splitter;

import io.georocket.ConfigConstants;
import io.vertx.core.Handler;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

/**
 * Abstract base class for commands that need to export data
 * @author Michel Kraemer
 */
public abstract class AbstractQueryCommand extends AbstractGeoRocketCommand {
  private static Logger log = LoggerFactory.getLogger(AbstractQueryCommand.class);
  
  /**
   * Convenience method to URL-encode a string
   * @param str the string
   * @return the encoded string
   */
  protected String urlencode(String str) {
    try {
      return URLEncoder.encode(str, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }
  
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
    String queryPath = prepareQuery(query, layer);
    HttpClient client = vertx.createHttpClient();
    String host = config().getString(ConfigConstants.HOST);
    int port = config().getInteger(ConfigConstants.PORT);
    HttpClientRequest request = client.get(port, host, "/store" + queryPath);
    request.exceptionHandler(t -> {
      error(t.getMessage());
      log.error("Could not query store", t);
      client.close();
      handler.handle(1);
    });
    request.handler(response -> {
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
    request.end();
  }

  /**
   * Prepare a query. Generate a query path for the given search query and layer.
   * @param query the search query (may be null)
   * @param layer the layer to export (may be null)
   * @return the query path
   */
  protected String prepareQuery(String query, String layer) {
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
      urlQuery = "?search=" + urlencode(query);
    }
    
    layer = Splitter.on('/').splitToList(layer).stream()
      .map(this::urlencode).collect(Collectors.joining("/"));
    
    return layer + urlQuery;
  }
}
