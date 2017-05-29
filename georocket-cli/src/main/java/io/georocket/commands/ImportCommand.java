package io.georocket.commands;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Paths;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.SystemUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tools.ant.DirectoryScanner;
import org.apache.tools.ant.Project;
import org.apache.tools.ant.types.FileSet;

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
  protected List<String> patterns;
  protected List<String> tags;
  protected List<String> properties;
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
      this.tags = Stream.of(tags.split(","))
        .map(t -> t.trim())
        .collect(Collectors.toList());
    }
  }

  /**
   * Set the properties to attach to the imported file
   * @param properties the properties
   */
  @OptionDesc(longName = "properties", shortName = "props",
      description = "comma-separated list of properties (key:value) to attach to the file(s)",
      argumentName = "PROPERTIES", argumentType = ArgumentType.STRING)
  public void setProperties(String properties) {
    if (properties == null || properties.isEmpty()) {
      this.properties = null;
    } else {
      this.properties = Splitter.on(",").trimResults().splitToList(properties);
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
        Project project = new Project();
        FileSet fs = new FileSet();
        fs.setDir(new File(root));
        fs.setIncludes(glob);
        DirectoryScanner ds = fs.getDirectoryScanner(project);
        Arrays.stream(ds.getIncludedFiles())
          .map(path -> Paths.get(root, path).toString())
          .forEach(queue::add);
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
        if (queueSize > 1) {
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

    // get the first file to import
    String path = files.poll();

    // print file name
    System.out.print("Importing " + Paths.get(path).getFileName() + " ... ");

    // import file
    importFile(path, client, vertx)
      .subscribe(v -> {
        System.out.println("done");
        // import next file in the queue
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
    return fs.rxOpen(path, openOptions)
      // get file size
      .flatMap(f -> fs.rxProps(path).map(props -> Pair.of(f, props.size())))
      // import file
      .flatMapObservable(f -> {
        ObservableFuture<Void> o = RxHelper.observableFuture();
        Handler<AsyncResult<Void>> handler = o.toHandler();
        AsyncFile file = (AsyncFile)f.getLeft().getDelegate();

        WriteStream<Buffer> out = client.getStoreClient().startImport(layer, tags, properties,
            Optional.of(f.getRight()), handler);

        AtomicBoolean fileClosed = new AtomicBoolean();

        Pump pump = Pump.pump(file, out);
        file.endHandler(v -> {
          file.close();
          out.end();
          fileClosed.set(true);
        });

        Handler<Throwable> exceptionHandler = t -> {
          if (!fileClosed.get()) {
            file.endHandler(null);
            file.close();
          }
          handler.handle(Future.failedFuture(t));
        };
        file.exceptionHandler(exceptionHandler);
        out.exceptionHandler(exceptionHandler);

        pump.start();
        return o;
    });
  }
}
