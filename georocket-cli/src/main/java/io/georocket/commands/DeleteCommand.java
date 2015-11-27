package io.georocket.commands;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;

import de.undercouch.underline.InputReader;
import de.undercouch.underline.Option.ArgumentType;
import de.undercouch.underline.OptionDesc;
import de.undercouch.underline.OptionParserException;
import de.undercouch.underline.UnknownAttributes;
import io.georocket.ConfigConstants;
import io.vertx.core.Handler;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

/**
 * Delete chunks or layers from the GeoRocket data store
 * @author Michel Kraemer
 */
public class DeleteCommand extends AbstractQueryCommand {
  private static Logger log = LoggerFactory.getLogger(DeleteCommand.class);
  
  private String query;
  private String layer;
  
  /**
   * Set the query parts
   * @param queryParts the query parts
   */
  @UnknownAttributes("QUERY")
  public void setQueryParts(List<String> queryParts) {
    this.query = String.join(" ", queryParts);
  }
  
  /**
   * Set the absolute path to the layer from which to delete
   * @param layer the layer
   */
  @OptionDesc(longName = "layer", shortName = "l",
      description = "absolute path to the layer from which to delete",
      argumentName = "PATH", argumentType = ArgumentType.STRING)
  public void setLayer(String layer) {
    this.layer = layer;
  }
  
  @Override
  public String getUsageName() {
    return "delete";
  }

  @Override
  public String getUsageDescription() {
    return "Delete from the GeoData store";
  }
  
  @Override
  public boolean checkArguments() {
    if ((query == null || query.isEmpty()) && (layer == null || layer.isEmpty())) {
      error("no search query and no layer given; do you really wish to delete the whole data store?");
      return false;
    }
    return super.checkArguments();
  }

  @Override
  public void doRun(String[] remainingArgs, InputReader in, PrintWriter out,
      Handler<Integer> handler) throws OptionParserException, IOException {
    String queryPath = prepareQuery(query, layer);
    HttpClient client = vertx.createHttpClient();
    String host = config().getString(ConfigConstants.HOST);
    int port = config().getInteger(ConfigConstants.PORT);
    HttpClientRequest request = client.delete(port, host, "/store" + queryPath);
    request.exceptionHandler(t -> {
      error(t.getMessage());
      log.error("Could not delete from store", t);
      client.close();
      handler.handle(1);
    });
    request.handler(response -> {
      if (response.statusCode() != 204) {
        error(response.statusMessage());
        client.close();
        handler.handle(1);
      } else {
        response.endHandler(v -> {
          client.close();
          handler.handle(0);
        });
      }
    });
    request.end();
  }
}
