package io.georocket.commands;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Queue;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.SystemUtils;
import org.apache.commons.lang3.tuple.Pair;

import com.google.common.base.Splitter;

import de.undercouch.underline.InputReader;
import de.undercouch.underline.Option.ArgumentType;
import de.undercouch.underline.OptionDesc;
import de.undercouch.underline.OptionParserException;
import de.undercouch.underline.UnknownAttributes;
import io.georocket.client.GeoRocketClient;
import io.georocket.util.DurationFormat;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.file.AsyncFile;
import io.vertx.core.file.OpenOptions;
import io.vertx.core.streams.Pump;
import io.vertx.core.streams.WriteStream;
import io.vertx.rx.java.ObservableFuture;
import io.vertx.rx.java.RxHelper;
import io.vertx.rxjava.core.Vertx;
import io.vertx.rxjava.core.file.FileSystem;
import rx.Observable;

/**
 * Import one or more files into GeoRocket
 * @author Michel Kraemer
 */
public class ImportCommand extends AbstractGeoRocketCommand {
  private List<String> patterns;
  protected List<String> tags;
  protected String layer;
  
  /**
   * Set the patterns of the files to import
   * @param patterns the file patterns
   */
  @UnknownAttributes("FILE PATTERN")
  public void setPatterns(List<String> patterns) {
    this.patterns = patterns;
  }
  
  /**
   * Set the tags to attach to the imported file
   * @param tags the tags
   */
  @OptionDesc(longName = "tags", shortName = "t",
      description = "comma-separated list of tags to attach to the file(s)",
      argumentName = "TAGS", argumentType = ArgumentType.STRING)
  public void setTags(String tags) {
    if (tags == null || tags.isEmpty()) {
      this.tags = null;
    } else {
      this.tags = Splitter.on(',').trimResults().splitToList(tags);
    }
  }
  
  /**
   * Set the absolute path to the layer to search
   * @param layer the layer
   */
  @OptionDesc(longName = "layer", shortName = "l",
      description = "absolute path to the destination layer",
      argumentName = "PATH", argumentType = ArgumentType.STRING)
  public void setLayer(String layer) {
    this.layer = layer;
  }
  
  @Override
  public String getUsageName() {
    return "import";
  }

  @Override
  public String getUsageDescription() {
    return "Import one or more files into GeoRocket";
  }
  
  @Override
  public boolean checkArguments() {
    if (patterns == null || patterns.isEmpty()) {
      error("no file pattern given. provide at least one file to import.");
      return false;
    }
    return super.checkArguments();
  }
  
  /**
   * Check if the given string contains a glob character ('*', '{', '?', or '[')
   * @param s the string
   * @return true if the string contains a glob character, false otherwise
   */
  private boolean hasGlobCharacter(String s) {
    for (int i = 0; i < s.length(); ++i) {
      char c = s.charAt(i);
      if (c == '\\') {
          ++i;
          continue;
      }
      if (c == '*' || c == '{' || c == '?' || c == '[') {
        return true;
      }
    }
    return false;
  }
  
  @Override
  public void doRun(String[] remainingArgs, InputReader in, PrintWriter out,
      Handler<Integer> handler) throws OptionParserException, IOException {
    long start = System.currentTimeMillis();
    
    // resolve file patterns
    Queue<String> queue = new ArrayDeque<>();
    for (String p : patterns) {
      // convert Windows backslashes to slashes (necessary for Files.newDirectoryStream())
      if (SystemUtils.IS_OS_WINDOWS) {
        p = FilenameUtils.separatorsToUnix(p);
      }
      
      // collect paths and glob patterns
      List<String> roots = new ArrayList<>();
      List<String> globs = new ArrayList<>();
      String[] parts = p.split("/");
      boolean rootParsed = false;
      for (String part : parts) {
        if (!rootParsed) {
          if (hasGlobCharacter(part)) {
            globs.add(part);
            rootParsed = true;
          } else {
            roots.add(part);
          }
        } else {
          globs.add(part);
        }
      }
      
      if (globs.isEmpty()) {
        // string does not contain a glob pattern at all
        queue.add(p);
      } else {
        // string contains a glob pattern
        if (roots.isEmpty()) {
          // there are not paths in the string. start from the current
          // working directory
          roots.add(".");
        }
        
        // add all files matching the pattern
        String root = String.join("/", roots);
        String glob = String.join("/", globs);
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(Paths.get(root), glob)) {
          stream.forEach(path -> queue.add(path.toString()));
        }
      }
    }
    
    if (queue.isEmpty()) {
      error("given pattern didn't match any files");
      return;
    }
    
    Vertx vertx = new Vertx(this.vertx);
    GeoRocketClient client = createClient();
    
    int queueSize = queue.size();
    doImport(queue, client, vertx, exitCode -> {
      client.close();
      
      if (exitCode == 0) {
        String m = "file";
        if (patterns.size() > 1) {
          m += "s";
        }
        System.out.println("Successfully imported " + queueSize + " " +
            m + " in " + DurationFormat.formatUntilNow(start));
      }
      
      handler.handle(exitCode);
    });
  }

  /**
   * Import files using a HTTP client and finally call a handler
   * @param files the files to import
   * @param client the GeoRocket client
   * @param vertx the Vert.x instance
   * @param handler the handler to call when all files have been imported
   */
  private void doImport(Queue<String> files, GeoRocketClient client,
      Vertx vertx, Handler<Integer> handler) {
    if (files.isEmpty()) {
      handler.handle(0);
      return;
    }
    
    String path = files.poll();

    // print file name
    System.out.print("Importing " + Paths.get(path).getFileName() + " ... ");

    importFile(path, client, vertx)
      .map(v -> {
        System.out.println("done");
        return v;
      })
      // handle response
      .subscribe(v -> {
        doImport(files, client, vertx, handler);
      }, err -> {
        System.out.println("error");
        error(err.getMessage());
        handler.handle(1);
      });
  }

  /**
   * Upload a file to GeoRocket
   * @param path path to file to import
   * @param client the GeoRocket client
   * @param vertx the Vert.x instance
   * @return an observable that will emit when the file has been uploaded
   */
  protected Observable<Void> importFile(String path, GeoRocketClient client, Vertx vertx) {

    // open file
    FileSystem fs = vertx.fileSystem();
    OpenOptions openOptions = new OpenOptions().setCreate(false).setWrite(false);
    return fs.openObservable(path, openOptions)
      // get file size
      .flatMap(f -> fs.propsObservable(path).map(props -> Pair.of(f, props.size())))
      // import file
      .flatMap(f -> {

        ObservableFuture<Void> o = RxHelper.observableFuture();
        Handler<AsyncResult<Void>> handler = o.toHandler();
        AsyncFile file = (AsyncFile)f.getLeft().getDelegate();

        WriteStream<Buffer> out = client.getStore().startImport(layer, tags,
                Optional.of(f.getRight()), handler);

        Pump pump = Pump.pump(file, out);
        file.endHandler(v -> {
          file.close();
          out.end();
        });

        Handler<Throwable> exceptionHandler = t -> {
          file.endHandler(null);
          file.close();
          out.end();
          handler.handle(Future.failedFuture(t));
        };
        file.exceptionHandler(exceptionHandler);
        out.exceptionHandler(exceptionHandler);

        pump.start();
        return o;
    });
  }
}
