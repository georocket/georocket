package io.georocket.commands;

import com.google.common.base.Splitter;
import de.undercouch.underline.InputReader;
import de.undercouch.underline.Option.ArgumentType;
import de.undercouch.underline.OptionDesc;
import de.undercouch.underline.OptionParserException;
import de.undercouch.underline.UnknownAttributes;
import io.georocket.client.GeoRocketClient;
import io.georocket.client.ImportParams;
import io.georocket.client.ImportParams.Compression;
import io.georocket.client.ImportResult;
import io.georocket.client.StoreClient;
import io.georocket.commands.console.ImportProgressRenderer;
import io.georocket.util.DurationFormat;
import io.georocket.util.SizeFormat;
import io.georocket.util.io.GzipWriteStream;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.file.OpenOptions;
import io.vertx.core.streams.WriteStream;
import io.vertx.rx.java.ObservableFuture;
import io.vertx.rx.java.RxHelper;
import io.vertx.rxjava.core.Vertx;
import io.vertx.rxjava.core.file.FileSystem;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.SystemUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tools.ant.DirectoryScanner;
import org.apache.tools.ant.Project;
import org.apache.tools.ant.types.FileSet;
import org.pcollections.PVector;
import org.pcollections.TreePVector;
import rx.Observable;
import rx.Single;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Import one or more files into GeoRocket
 * @author Michel Kraemer
 */
public class ImportCommand extends AbstractGeoRocketCommand {
  protected List<String> patterns;
  protected List<String> tags;
  protected List<String> properties;
  protected String layer;
  protected String fallbackCRS;

  private static class Metrics {
    final long bytesImported;
    final long bytesTransferred;

    Metrics(long bytesImported, long bytesTransferred) {
      this.bytesImported = bytesImported;
      this.bytesTransferred = bytesTransferred;
    }
  }
  
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
        .map(String::trim)
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

  /**
   * Set the fallback crs if the file does not specify a CRS
   * @param fallbackCRS the fallback CRS
   */
  @OptionDesc(longName = "fallbackCRS", shortName = "c",
      description = "the CRS to use for indexing if the file does not specify one",
      argumentName = "CRS", argumentType = ArgumentType.STRING)
  public void setFallbackCRS(String fallbackCRS) {
    this.fallbackCRS = fallbackCRS;
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
    List<String> files = new ArrayList<>();
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
        files.add(p);
      } else {
        // string contains a glob pattern
        if (roots.isEmpty()) {
          // there are no paths in the string. start from the current
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
          .forEach(files::add);
      }
    }
    
    if (files.isEmpty()) {
      error("given pattern didn't match any files");
      return;
    }
    
    Vertx vertx = new Vertx(this.vertx);
    GeoRocketClient client = createClient();
    
    doImport(files, client, vertx)
      .doAfterTerminate(client::close)
      .subscribe(metrics -> {
        String m = "file";
        if (files.size() > 1) {
          m += "s";
        }
        System.out.println("Successfully imported " + files.size() + " " + m);
        System.out.println("  Total time:         " + DurationFormat.formatUntilNow(start));
        System.out.println("  Total data size:    " + SizeFormat.format(metrics.bytesImported));
        System.out.println("  Transferred size:   " + SizeFormat.format(metrics.bytesTransferred));
        handler.handle(0);
      }, err -> {
        error(err.getMessage());
        handler.handle(1);
      });
  }

  /**
   * Determine the sizes of all given files
   * @param files the files
   * @param vertx the Vert.x instance
   * @return an observable that emits pairs of file names and sizes
   */
  private Observable<Pair<String, Long>> getFileSizes(List<String> files, Vertx vertx) {
    FileSystem fs = vertx.fileSystem();
    return Observable.from(files)
      .flatMapSingle(path -> fs.rxProps(path).map(props -> Pair.of(path, props.size())));
  }

  /**
   * Import files using a HTTP client and finally call a handler
   * @param files the files to import
   * @param client the GeoRocket client
   * @param vertx the Vert.x instance
   * @return an observable that will emit metrics when all files have been imported
   */
  private Single<Metrics> doImport(List<String> files, GeoRocketClient client,
      Vertx vertx) {
    ImportProgressRenderer progress = ImportProgressRenderer.create(vertx.getDelegate());
    progress.setTotalFiles(files.size());

    return getFileSizes(files, vertx)
      .reduce(Pair.<Long, PVector<Pair<String, Long>>>of(0L, TreePVector.empty()), (a, b) -> {
        long newTotalSize = a.getKey() + b.getValue();
        PVector<Pair<String, Long>> newPairs = a.getValue().plus(b);
        return Pair.of(newTotalSize, newPairs);
      })
      .flatMap(l -> {
        long totalSize = l.getKey();
        progress.setTotalSize(totalSize);

        return Observable.from(l.getValue())
          .zipWith(Observable.range(1, Integer.MAX_VALUE), Pair::of)
          .flatMapSingle(pair -> {
            String path = pair.getKey().getKey();
            Long size = pair.getKey().getValue();
            Integer index = pair.getValue();

            progress
              .startNewFile(Paths.get(path).getFileName().toString())
              .setIndex(index)
              .setSize(size);

            return importFile(path, size, progress, client, vertx);
          }, false, 1);
      })
      .reduce(new Metrics(0L, 0L), (a, b) ->
        new Metrics(a.bytesImported + b.bytesImported, a.bytesTransferred + b.bytesTransferred)
      )
      .toSingle()
      .doAfterTerminate(progress::dispose);
  }

  /**
   * Upload a file to GeoRocket
   * @param path the path to the file to import
   * @param fileSize the size of the file
   * @param progress a renderer that display the progress on the terminal
   * @param client the GeoRocket client
   * @param vertx the Vert.x instance
   * @return an observable that will emit metrics when the file has been uploaded
   */
  protected Single<Metrics> importFile(String path, long fileSize,
      ImportProgressRenderer progress, GeoRocketClient client, Vertx vertx) {
    // open file
    FileSystem fs = vertx.fileSystem();
    OpenOptions openOptions = new OpenOptions().setCreate(false).setWrite(false);
    return fs.rxOpen(path, openOptions)
      .flatMap(file -> {
        ObservableFuture<ImportResult> o = RxHelper.observableFuture();
        Handler<AsyncResult<ImportResult>> handler = o.toHandler();

        // start import
        ImportParams options = new ImportParams()
          .setLayer(layer)
          .setTags(tags)
          .setProperties(properties)
          .setFallbackCRS(fallbackCRS)
          .setCompression(Compression.GZIP);

        StoreClient store = client.getStore();
        WriteStream<Buffer> out;
        boolean alreadyCompressed = path.toLowerCase().endsWith(".gz");
        if (alreadyCompressed) {
          options.setSize(fileSize);
          out = store.startImport(options, handler);
        } else {
          out = new GzipWriteStream(store.startImport(options, handler));
        }

        AtomicBoolean fileClosed = new AtomicBoolean();
        AtomicLong bytesWritten = new AtomicLong();

        file.endHandler(v -> {
          file.close();
          out.end();
          fileClosed.set(true);
          progress.setCurrent(bytesWritten.get());
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

        file.handler(data -> {
          out.write(data.getDelegate());
          progress.setCurrent(bytesWritten.getAndAdd(data.length()));
          if (out.writeQueueFull()) {
            file.pause();
            out.drainHandler(v -> file.resume());
          }
        });

        return o.map(v -> {
          if (alreadyCompressed) {
            return new Metrics(fileSize, fileSize);
          }
          return new Metrics(fileSize, ((GzipWriteStream)out).getBytesWritten());
        }).toSingle();
    });
  }
}
