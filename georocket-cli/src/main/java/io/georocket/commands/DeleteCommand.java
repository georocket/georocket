package io.georocket.commands;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;

import de.undercouch.underline.InputReader;
import de.undercouch.underline.Option.ArgumentType;
import de.undercouch.underline.OptionDesc;
import de.undercouch.underline.OptionParserException;
import de.undercouch.underline.UnknownAttributes;
import io.georocket.client.GeoRocketClient;
import io.vertx.core.Handler;
import io.vertx.core.impl.NoStackTraceThrowable;
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
    return "Delete from the GeoRocket data store";
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
    GeoRocketClient client = createClient();
    client.getStore().delete(query, layer, ar -> {
      if (ar.failed()) {
        client.close();
        Throwable t = ar.cause();
        error(t.getMessage());
        if (!(t instanceof NoStackTraceThrowable)) {
          log.error("Could not delete from store", t);
        }
        handler.handle(1);
      } else {
        handler.handle(0);
      }
    });
  }
}
