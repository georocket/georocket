package io.georocket.commands;

import de.undercouch.underline.InputReader;
import de.undercouch.underline.OptionDesc;
import de.undercouch.underline.OptionParserException;
import de.undercouch.underline.UnknownAttributes;
import io.georocket.client.SearchParams;
import io.vertx.core.Handler;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;

/**
 * Exports a layer or the whole data store
 * @author Michel Kraemer
 */
public class ExportCommand extends AbstractQueryCommand {
  protected String layer;
  protected boolean optimisticMerging;
  
  /**
   * Set the absolute path to the layer to export
   * @param layer the layer
   */
  @UnknownAttributes("LAYER")
  public void setLayer(List<String> layer) {
    this.layer = String.join(" ", layer).trim();
    if (!this.layer.isEmpty()) {
      if (!this.layer.endsWith("/")) {
        this.layer += "/";
      }
      if (!this.layer.startsWith("/")) {
        this.layer = "/" + this.layer;
      }
    }
  }

  /**
   * Enable optimistic merging
   * @param optimisticMerging {@code true} if optimistic merging should
   * be enabled
   */
  @OptionDesc(longName = "optimistic-merging",
      description = "enable optimistic merging")
  public void setOptimisticMerging(boolean optimisticMerging) {
    this.optimisticMerging = optimisticMerging;
  }
  
  @Override
  public String getUsageName() {
    return "export";
  }

  @Override
  public String getUsageDescription() {
    return "Export a layer or the whole data store";
  }

  @Override
  public void doRun(String[] remainingArgs, InputReader in, PrintWriter out, Handler<Integer> handler)
      throws OptionParserException, IOException {
    SearchParams params = new SearchParams()
        .setLayer(layer)
        .setOptimisticMerging(optimisticMerging);
    query(params, out, handler);
  }
}
