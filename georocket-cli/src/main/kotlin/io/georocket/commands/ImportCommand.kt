package io.georocket.commands

import de.undercouch.underline.InputReader
import de.undercouch.underline.Option.ArgumentType
import de.undercouch.underline.OptionDesc
import de.undercouch.underline.UnknownAttributes
import io.georocket.client.GeoRocketClient
import io.georocket.client.ImportParams
import io.georocket.client.ImportParams.Compression
import io.georocket.client.ImportResult
import io.georocket.commands.console.ImportProgressRenderer
import io.georocket.util.SizeFormat
import io.georocket.util.formatUntilNow
import io.georocket.util.io.GzipWriteStream
import io.vertx.core.Future
import io.vertx.core.Handler
import io.vertx.core.buffer.Buffer
import io.vertx.core.file.OpenOptions
import io.vertx.core.streams.WriteStream
import io.vertx.rx.java.RxHelper
import io.vertx.rxjava.core.Vertx
import org.apache.commons.io.FilenameUtils
import org.apache.commons.lang3.SystemUtils
import org.apache.tools.ant.Project
import org.apache.tools.ant.types.FileSet
import org.pcollections.TreePVector
import rx.Observable
import rx.Single
import java.io.File
import java.io.PrintWriter
import java.nio.file.Paths
import java.util.ArrayList
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong

/**
 * Import one or more files into GeoRocket
 */
class ImportCommand : AbstractGeoRocketCommand() {
  private var tags: List<String>? = null
  private var properties: List<String>? = null

  override val usageName = "import"
  override val usageDescription = "Import one or more files into GeoRocket"

  private data class Metrics(val bytesImported: Long, val bytesTransferred: Long)

  /**
   * The patterns of the files to import
   */
  @set:UnknownAttributes("FILE PATTERN")
  var patterns = emptyList<String>()

  @set:OptionDesc(longName = "layer", shortName = "l",
      description = "absolute path to the destination layer",
      argumentName = "PATH", argumentType = ArgumentType.STRING)
  var layer: String? = null

  @set:OptionDesc(longName = "fallbackCRS", shortName = "c",
      description = "the CRS to use for indexing if the file does not specify one",
      argumentName = "CRS", argumentType = ArgumentType.STRING)
  var fallbackCRS: String? = null

  /**
   * Set the tags to attach to the imported file
   */
  @OptionDesc(longName = "tags", shortName = "t",
      description = "comma-separated list of tags to attach to the file(s)",
      argumentName = "TAGS", argumentType = ArgumentType.STRING)
  @Suppress("UNUSED")
  fun setTags(tags: String?) {
    if (tags == null || tags.isEmpty()) {
      this.tags = null
    } else {
      this.tags = tags.split(",")
          .map { it.trim() }
          .filter { it.isNotEmpty() }
          .toList()
    }
  }

  /**
   * Set the properties to attach to the imported file
   */
  @OptionDesc(longName = "properties", shortName = "props",
      description = "comma-separated list of properties (key:value) to attach to the file(s)",
      argumentName = "PROPERTIES", argumentType = ArgumentType.STRING)
  @Suppress("UNUSED")
  fun setProperties(properties: String?) {
    if (properties == null || properties.isEmpty()) {
      this.properties = null
    } else {
      this.properties = properties.split(",")
          .map { it.trim() }
          .filter { it.isNotEmpty() }
          .toList()
    }
  }

  override fun checkArguments(): Boolean {
    if (patterns.isEmpty()) {
      error("no file pattern given. provide at least one file to import.")
      return false
    }
    return super.checkArguments()
  }

  /**
   * Check if the given string contains a glob character ('*', '{', '?', or '[')
   * @param s the string
   * @return true if the string contains a glob character, false otherwise
   */
  private fun hasGlobCharacter(s: String): Boolean {
    var i = 0
    while (i < s.length) {
      val c = s[i]
      if (c == '\\') {
        ++i
      } else if (c == '*' || c == '{' || c == '?' || c == '[') {
        return true
      }
      ++i
    }
    return false
  }

  override fun doRun(remainingArgs: Array<String>, i: InputReader,
      o: PrintWriter, handler: Handler<Int>) {
    val start = System.currentTimeMillis()

    // resolve file patterns
    val files = ArrayList<String>()
    for (p in patterns) {
      // convert Windows backslashes to slashes (necessary for Files.newDirectoryStream())
      val pattern = if (SystemUtils.IS_OS_WINDOWS) {
        FilenameUtils.separatorsToUnix(p)
      } else {
        p
      }

      // collect paths and glob patterns
      val roots = ArrayList<String>()
      val globs = ArrayList<String>()
      val parts = pattern.split("/")
      var rootParsed = false
      for (part in parts) {
        if (!rootParsed) {
          if (hasGlobCharacter(part)) {
            globs.add(part)
            rootParsed = true
          } else {
            roots.add(part)
          }
        } else {
          globs.add(part)
        }
      }

      if (globs.isEmpty()) {
        // string does not contain a glob pattern at all
        files.add(pattern)
      } else {
        // string contains a glob pattern
        if (roots.isEmpty()) {
          // there are no paths in the string. start from the current
          // working directory
          roots.add(".")
        }

        // add all files matching the pattern
        val root = roots.joinToString("/")
        val glob = globs.joinToString("/")
        val project = Project()
        val fs = FileSet()
        fs.dir = File(root)
        fs.setIncludes(glob)
        val ds = fs.getDirectoryScanner(project)
        ds.includedFiles.mapTo(files) { Paths.get(root, it).toString() }
      }
    }

    if (files.isEmpty()) {
      error("given pattern didn't match any files")
      return
    }

    val vertx = Vertx(this.vertx)
    val client = createClient()

    doImport(files, client, vertx)
        .doAfterTerminate { client.close() }
        .subscribe({ metrics ->
          var m = "file"
          if (files.size > 1) {
            m += "s"
          }
          println("Successfully imported ${files.size} m")
          println("  Total time:         ${start.formatUntilNow()}")
          println("  Total data size:    ${SizeFormat.format(metrics.bytesImported)}")
          println("  Transferred size:   ${SizeFormat.format(metrics.bytesTransferred)}")
          handler.handle(0)
        }, { err ->
          error(err.message)
          handler.handle(1)
        })
  }

  /**
   * Determine the sizes of all given files
   * @param files the files
   * @param vertx the Vert.x instance
   * @return an observable that emits pairs of file names and sizes
   */
  private fun getFileSizes(files: List<String>, vertx: Vertx): Observable<Pair<String, Long>> {
    val fs = vertx.fileSystem()
    return Observable.from(files)
        .flatMapSingle { path -> fs.rxProps(path).map { Pair(path, it.size()) } }
  }

  /**
   * Import files using a HTTP client and finally call a handler
   * @param files the files to import
   * @param client the GeoRocket client
   * @param vertx the Vert.x instance
   * @return an observable that will emit metrics when all files have been imported
   */
  private fun doImport(files: List<String>, client: GeoRocketClient,
      vertx: Vertx): Single<Metrics> {
    val progress = ImportProgressRenderer(vertx.delegate)
    progress.totalFiles = files.size

    return getFileSizes(files, vertx)
        .reduce(Pair(0L, TreePVector.empty<Pair<String, Long>>())) { a, b ->
          val newTotalSize = a.first + b.second
          val newPairs = a.second.plus(b)
          Pair(newTotalSize, newPairs)
        }
        .flatMap { l ->
          val totalSize = l.first
          progress.totalSize = totalSize

          Observable.from(l.second)
              .zipWith(Observable.range(1, Integer.MAX_VALUE)) { left, right -> Pair(left, right) }
              .flatMapSingle({ pair ->
                val path = pair.first.first
                val size = pair.first.second
                val index = pair.second

                progress.startNewFile(Paths.get(path).fileName.toString())
                progress.index = index
                progress.size = size

                importFile(path, size, progress, client, vertx)
              }, false, 1)
        }
        .reduce(Metrics(0L, 0L)) { a, b ->
          Metrics(a.bytesImported + b.bytesImported, a.bytesTransferred + b.bytesTransferred)
        }
        .toSingle()
        .doAfterTerminate { progress.dispose() }
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
  private fun importFile(path: String, fileSize: Long,
      progress: ImportProgressRenderer, client: GeoRocketClient,
      vertx: Vertx): Single<Metrics> {
    // open file
    val fs = vertx.fileSystem()
    val openOptions = OpenOptions().setCreate(false).setWrite(false)
    return fs.rxOpen(path, openOptions)
        .flatMap { file ->
          val o = RxHelper.observableFuture<ImportResult>()
          val handler = o.toHandler()

          // start import
          val options = ImportParams()
              .setLayer(layer)
              .setTags(tags)
              .setProperties(properties)
              .setFallbackCRS(fallbackCRS)
              .setCompression(Compression.GZIP)

          val store = client.store
          val out: WriteStream<Buffer>
          val alreadyCompressed = path.toLowerCase().endsWith(".gz")
          if (alreadyCompressed) {
            options.size = fileSize
            out = store.startImport(options, handler)
          } else {
            out = GzipWriteStream(store.startImport(options, handler))
          }

          val fileClosed = AtomicBoolean()
          val bytesWritten = AtomicLong()

          file.endHandler {
            file.close()
            out.end()
            fileClosed.set(true)
            progress.current = bytesWritten.get()
          }

          val exceptionHandler = { t: Throwable ->
            if (!fileClosed.get()) {
              file.endHandler(null)
              file.close()
            }
            handler.handle(Future.failedFuture(t))
          }
          file.exceptionHandler(exceptionHandler)
          out.exceptionHandler(exceptionHandler)

          file.handler { data ->
            out.write(data.delegate)
            progress.current = bytesWritten.getAndAdd(data.length().toLong())
            if (out.writeQueueFull()) {
              file.pause()
              out.drainHandler { file.resume() }
            }
          }

          o.map {
            if (alreadyCompressed) {
              Metrics(fileSize, fileSize)
            } else {
              Metrics(fileSize, (out as GzipWriteStream).bytesWritten)
            }
          }.toSingle()
        }
  }
}
