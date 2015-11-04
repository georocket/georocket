package de.fhg.igd.georocket.commands;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.URLEncoder;
import java.util.List;

import de.undercouch.underline.InputReader;
import de.undercouch.underline.Option.ArgumentType;
import de.undercouch.underline.OptionDesc;
import de.undercouch.underline.OptionParserException;
import de.undercouch.underline.UnknownAttributes;
import io.vertx.core.Handler;
import io.vertx.core.http.HttpClient;

/**
 * Searches the GeoRocket data store and outputs the retrieved files
 * @author Michel Kraemer
 */
public class SearchCommand extends AbstractGeoRocketCommand {
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
   * Set the absolute path to the layer to search
   * @param layer the layer
   */
  @OptionDesc(longName = "layer", shortName = "l",
      description = "absolute path to the layer to search",
      argumentName = "PATH", argumentType = ArgumentType.STRING)
  public void setLayer(String layer) {
    this.layer = layer;
    if (layer == null || layer.isEmpty()) {
      return;
    }
    if (!this.layer.endsWith("/")) {
      this.layer += "/";
    }
    if (!this.layer.startsWith("/")) {
      this.layer = "/" + this.layer;
    }
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
  public void doRun(String[] remainingArgs, InputReader in, PrintWriter out, Handler<Integer> handler)
      throws OptionParserException, IOException {
    String urlQuery = URLEncoder.encode(query, "UTF-8");
    
    if (layer == null || layer.isEmpty()) {
      layer = "/";
    }
    
    HttpClient client = vertx.createHttpClient();
    client.getNow(63074, "localhost", "/store" + layer + "?search=" + urlQuery, response -> {
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
