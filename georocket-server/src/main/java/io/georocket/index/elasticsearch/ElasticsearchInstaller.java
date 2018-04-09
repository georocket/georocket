package io.georocket.index.elasticsearch;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Enumeration;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.SystemUtils;

import io.georocket.util.HttpException;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.file.OpenOptions;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.rx.java.ObservableFuture;
import io.vertx.rx.java.RxHelper;
import io.vertx.rxjava.core.Vertx;
import io.vertx.rxjava.core.file.AsyncFile;
import io.vertx.rxjava.core.file.FileSystem;
import io.vertx.rxjava.core.http.HttpClient;
import io.vertx.rxjava.core.http.HttpClientRequest;
import rx.Completable;
import rx.Single;

/**
 * Download and extract Elasticsearch to a given location if it does not exist
 * there yet
 * @author Michel Kraemer
 */
public class ElasticsearchInstaller {
  private static Logger log = LoggerFactory.getLogger(ElasticsearchInstaller.class);
  
  private final Vertx vertx;
  
  /**
   * Create a new Elasticsearch installer
   * @param vertx a Vert.x instance
   */
  public ElasticsearchInstaller(Vertx vertx) {
    this.vertx = vertx;
  }
  
  /**
   * Download and extract Elasticsearch to a given location. If the destination
   * exists already this method does nothing.
   * @param downloadUrl the URL to the ZIP file containing Elasticsearch
   * @param destPath the path to the destination folder where the downloaded ZIP
   * file should be extracted to
   * @return a single emitting the path to the extracted Elasticsearch
   */
  public Single<String> download(String downloadUrl, String destPath) {
    return download(downloadUrl, destPath, true);
  }
  
  /**
   * Download and extract Elasticsearch to a given location. If the destination
   * exists already this method does nothing.
   * @param downloadUrl the URL to the ZIP file containing Elasticsearch
   * @param destPath the path to the destination folder where the downloaded ZIP
   * file should be extracted to
   * @param strip <code>true</code> if the first path element of all items in the
   * ZIP file should be stripped away.
   * @return a single emitting the path to the extracted Elasticsearch
   */
  public Single<String> download(String downloadUrl, String destPath,
      boolean strip) {
    FileSystem fs = vertx.fileSystem();
    return fs.rxExists(destPath)
        .flatMap(exists -> {
          if (exists) {
            return Single.just(destPath);
          }
          return downloadNoCheck(downloadUrl, destPath, strip);
        });
  }
  
  /**
   * Download and extract Elasticsearch to a given location no matter if the
   * destination folder exists or not.
   * @param downloadUrl the URL to the ZIP file containing Elasticsearch
   * @param destPath the path to the destination folder where the downloaded ZIP
   * file should be extracted to
   * @param strip <code>true</code> if the first path element of all items in the
   * ZIP file should be stripped away.
   * @return emitting the path to the extracted Elasticsearch
   */
  private Single<String> downloadNoCheck(String downloadUrl, String destPath,
      boolean strip) {
    log.info("Downloading Elasticsearch ...");
    log.info("Source: " + downloadUrl);
    log.info("Dest: " + destPath);
    
    // download the archive, extract it and finally delete it
    return downloadArchive(downloadUrl)
        .flatMap(archivePath -> {
          return extractArchive(archivePath, destPath, strip)
              .doAfterTerminate(() -> {
                FileSystem fs = vertx.fileSystem();
                fs.deleteBlocking(archivePath);
              });
        });
  }
  
  /**
   * Download the ZIP file containing Elasticsearch to a temporary location
   * @param downloadUrl the URL to the ZIP file
   * @return a single emitting the location of the temporary file
   */
  private Single<String> downloadArchive(String downloadUrl) {
    // create temporary file
    File archiveFile;
    try {
      archiveFile = File.createTempFile("elasticsearch", "tmp");
    } catch (IOException e) {
      return Single.error(e);
    }
    
    // open temporary file
    FileSystem fs = vertx.fileSystem();
    String archivePath = archiveFile.getAbsolutePath();
    OpenOptions openOptions = new OpenOptions()
        .setCreate(true)
        .setWrite(true)
        .setTruncateExisting(true);
    return fs.rxOpen(archivePath, openOptions)
        .flatMap(file -> {
          return doDownload(downloadUrl, file)
            .doAfterTerminate(() -> {
              file.flush();
              file.close();
            })
            .toSingleDefault(archivePath);
        });
  }
  
  /**
   * Download a file
   * @param downloadUrl the URL to download from
   * @param dest the destination file
   * @return a Completable that will complete once the file has been downloaded
   */
  private Completable doDownload(String downloadUrl, AsyncFile dest) {
    ObservableFuture<Void> observable = RxHelper.observableFuture();
    Handler<AsyncResult<Void>> handler = observable.toHandler();
    
    HttpClientOptions options = new HttpClientOptions();
    if (downloadUrl.startsWith("https")) {
      options.setSsl(true);
    }
    HttpClient client = vertx.createHttpClient(options);
    HttpClientRequest req = client.getAbs(downloadUrl);
    
    req.exceptionHandler(t -> handler.handle(Future.failedFuture(t)));
    
    req.handler(res -> {
      if (res.statusCode() != 200) {
        handler.handle(Future.failedFuture(new HttpException(res.statusCode(),
            res.statusMessage())));
        return;
      }
      
      // get content-length
      int length;
      int[] read = { 0 };
      int[] lastOutput = { 0 };
      String contentLength = res.getHeader("Content-Length");
      if (contentLength != null) {
        length = Integer.parseInt(contentLength);
      } else {
        length = 0;
      }
      
      res.exceptionHandler(t -> handler.handle(Future.failedFuture(t)));
      
      // download file contents, log progress
      res.handler(buf -> {
        read[0] += buf.length();
        if (lastOutput[0] == 0 || read[0] - lastOutput[0] > 1024 * 2048) {
          logProgress(length, read[0]);
          lastOutput[0] = read[0];
        }
        dest.write(buf);
      });
      
      res.endHandler(v -> {
        logProgress(length, read[0]);
        handler.handle(Future.succeededFuture());
      });
    });
    
    req.end();
    
    return observable.toCompletable();
  }

  /**
   * Log progress of a file download
   * @param length the length of the file (may be 0 if unknown)
   * @param read the number of bytes downloaded so far
   */
  private void logProgress(int length, int read) {
    if (length > 0) {
      log.info("Downloaded " + read + "/" + length + " bytes (" +
          Math.round(read * 100.0 / length) + "%)");
    } else {
      log.info("Downloaded " + read + " bytes");
    }
  }
  
  /**
   * Extract the Elasticsearch ZIP archive to a destination path
   * @param archivePath the path to the ZIP file
   * @param destPath the destination path
   * @param strip <code>true</code> if the first path element of all items in the
   * ZIP file should be stripped away.
   * @return emitting the path to the extracted contents (i.e.
   * <code>destPath</code>)
   */
  private Single<String> extractArchive(String archivePath, String destPath,
      boolean strip) {
    ObservableFuture<String> observable = RxHelper.observableFuture();
    Handler<AsyncResult<String>> handler = observable.toHandler();
    
    // extract archive asynchronously
    vertx.executeBlocking(f -> {
      File archiveFile = new File(archivePath);
      File destFile = new File(destPath);
      destFile.mkdirs();
      try {
        extractZip(archiveFile, destFile, strip);
        f.complete();
      } catch (IOException e) {
        FileUtils.deleteQuietly(destFile);
        f.fail(e);
      }
    }, ar -> {
      if (ar.failed()) {
        handler.handle(Future.failedFuture(ar.cause()));
      } else {
        handler.handle(Future.succeededFuture(destPath));
      }
    });
    
    // set executable permissions for Elasticsearch binary
    return observable.doOnNext(path -> {
      if (!SystemUtils.IS_OS_WINDOWS) {
        log.info("Set executable permissions for \"bin/elasticsearch\"");
        File archiveFile = new File(path);
        File executable = new File(archiveFile, "bin/elasticsearch");
        executable.setExecutable(true);
      }
    }).toSingle();
  }
  
  /**
   * Extract a ZIP file to a destination directory. This method is blocking!
   * @param file the ZIP file to extract
   * @param destDir the destination directory
   * @param strip <code>true</code> if the first path element of all items in the
   * ZIP file should be stripped away.
   * @throws IOException if an I/O error occurred while extracting
   */
  private void extractZip(File file, File destDir, boolean strip) throws IOException {
    log.info("Extracting archive ... 0%");
    
    // open ZIP file
    try (ZipFile zipFile = new ZipFile(file)) {
      Enumeration<? extends ZipEntry> entries = zipFile.entries();
      int count = 0;
      
      // extract all elements
      while (entries.hasMoreElements()) {
        ZipEntry entry = entries.nextElement();
        String name;
        
        // strip first path element
        if (strip) {
          name = entry.getName().substring(FilenameUtils.separatorsToUnix(
              entry.getName()).indexOf('/') + 1);
        } else {
          name = entry.getName();
        }
        
        File dest = new File(destDir, name);
        if (entry.isDirectory()) {
          // create directories
          dest.mkdirs();
        } else {
          // extract files
          dest.getParentFile().mkdirs();
          try (InputStream in = zipFile.getInputStream(entry);
              OutputStream out = new FileOutputStream(dest)) {
            IOUtils.copy(in, out);
          }
        }
        
        // log progress
        count++;
        long p = Math.round(count * 100.0 / zipFile.size());
        if (p % 10 == 0) {
          log.info("Extracting archive ... " + p + "%");
        }
      }
    }
  }
}
