package io.georocket.commands;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;

import de.undercouch.underline.InputReader;
import de.undercouch.underline.Option.ArgumentType;
import de.undercouch.underline.OptionDesc;
import de.undercouch.underline.OptionParserException;
import de.undercouch.underline.UnknownAttributes;
import io.vertx.core.Handler;

/**
 * Searches the GeoRocket data store and outputs the retrieved files
 * @author Michel Kraemer
 */
public class SearchCommand extends AbstractQueryCommand {
  protected String query;
  protected String layer;
  
  /**
   * Set the query parts
   * @param queryParts the query parts
   */
  @UnknownAttributes("QUERY")
  public void setQueryParts(List<String> queryParts) {
    this.query = String.join(" ", queryParts);
  }
  
  /**
   * Set the absolute path to the layer to search
   * @param layer the layer
   */
  @OptionDesc(longName = "layer", shortName = "l",
      description = "absolute path to the layer to search",
      argumentName = "PATH", argumentType = ArgumentType.STRING)
  public void setLayer(String layer) {
    this.layer = layer;
  }
  
  @Override
  public String getUsageName() {
    return "search";
  }

  @Override
  public String getUsageDescription() {
    return "Search the GeoRocket data store";
  }
  
  @Override
  public boolean checkArguments() {
    if (query == null || query.isEmpty()) {
      error("no search query given");
      return false;
    }
    return super.checkArguments();
  }

  @Override
  public void doRun(String[] remainingArgs, InputReader in, PrintWriter out,
      Handler<Integer> handler) throws OptionParserException, IOException {
    query(query, layer, out, handler);
  }
}
