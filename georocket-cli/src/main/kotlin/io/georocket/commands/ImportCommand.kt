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
import io.georocket.util.coroutines.awaitBlockingConcurrent
import io.georocket.util.formatUntilNow
import io.vertx.core.Future
import io.vertx.core.buffer.Buffer
import io.vertx.core.file.OpenOptions
import io.vertx.core.streams.ReadStream
import io.vertx.kotlin.core.file.openAwait
import io.vertx.kotlin.core.file.propsAwait
import io.vertx.kotlin.coroutines.await
import io.vertx.kotlin.coroutines.awaitBlocking
import io.vertx.kotlin.coroutines.toChannel
import org.apache.commons.io.FilenameUtils
import org.apache.commons.lang3.SystemUtils
import org.apache.tools.ant.Project
import org.apache.tools.ant.types.FileSet
import java.io.ByteArrayOutputStream
import java.io.File
import java.io.PrintWriter
import java.nio.file.Paths
import java.util.ArrayList
import java.util.zip.GZIPOutputStream

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

  override suspend fun doRun(remainingArgs: Array<String>, i: InputReader,
      o: PrintWriter): Int {
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
      return 1
    }

    return try {
      val metrics = createClient().use { doImport(files, it) }

      var m = "file"
      if (files.size > 1) {
        m += "s"
      }
      println("Successfully imported ${files.size} $m")
      println("  Total time:         ${start.formatUntilNow()}")
      println("  Total data size:    ${SizeFormat.format(metrics.bytesImported)}")
      println("  Transferred size:   ${SizeFormat.format(metrics.bytesTransferred)}")

      0
    } catch (t: Throwable) {
      error(t.message)
      1
    }
  }

  /**
   * Determine the sizes of all given files
   * @param files the files
   * @return a list of pairs containing file names and sizes
   */
  private suspend fun getFileSizes(files: List<String>): List<Pair<String, Long>> {
    val fs = vertx.fileSystem()
    return files.map { path ->
      val props = fs.propsAwait(path)
      Pair(path, props.size())
    }
  }

  /**
   * Import files using a HTTP client and finally call a handler
   * @param files the files to import
   * @param client the GeoRocket client
   * @return an observable that will emit metrics when all files have been imported
   */
  private suspend fun doImport(files: List<String>, client: GeoRocketClient): Metrics {
    ImportProgressRenderer(vertx).use { progress ->
      progress.totalFiles = files.size

      val filesWithSizes = getFileSizes(files)
      val totalSize = filesWithSizes.map { it.second }.sum()
      progress.totalSize = totalSize

      var bytesImported = 0L
      var bytesTransferred = 0L
      for (file in filesWithSizes.withIndex()) {
        val path = file.value.first
        val size = file.value.second
        val index = file.index

        progress.startNewFile(Paths.get(path).fileName.toString())
        progress.index = index
        progress.size = size

        val m = importFile(path, size, progress, client)
        bytesImported += m.bytesImported
        bytesTransferred += m.bytesTransferred
      }

      return Metrics(bytesImported, bytesTransferred)
    }
  }

  /**
   * Upload a file to GeoRocket
   * @param path the path to the file to import
   * @param fileSize the size of the file
   * @param progress a renderer that display the progress on the terminal
   * @param client the GeoRocket client
   * @return a metrics object
   */
  private suspend fun importFile(path: String, fileSize: Long,
      progress: ImportProgressRenderer, client: GeoRocketClient): Metrics {
    // open file
    val fs = vertx.fileSystem()
    val openOptions = OpenOptions().setCreate(false).setWrite(false)
    val file = fs.openAwait(path, openOptions)
    try {
      // start import
      val options = ImportParams()
      options.layer = layer
      options.tags = tags
      options.properties = properties
      options.fallbackCRS = fallbackCRS
      options.compression = Compression.GZIP

      val alreadyCompressed = path.endsWith(".gz", true)
      val resultFuture = Future.future<ImportResult>()
      val out = client.store.startImport(options, resultFuture).toChannel(vertx)

      val baos = ByteArrayOutputStream()
      val os = if (alreadyCompressed) {
        options.size = fileSize
        baos
      } else {
        awaitBlockingConcurrent { GZIPOutputStream(baos) }
      }

      var bytesWritten = 0L
      var compressedBytesWritten = 0L
      val fileChannel = (file as ReadStream<Buffer>).toChannel(vertx)
      for (buf in fileChannel) {
        awaitBlocking { os.write(buf.bytes) }
        if (baos.size() > 0) {
          out.send(Buffer.buffer(baos.toByteArray()))
          compressedBytesWritten += baos.size()
          baos.reset()
        }
        progress.current = bytesWritten
        bytesWritten += buf.length().toLong()
      }

      awaitBlocking { os.close() }
      if (baos.size() > 0) {
        out.send(Buffer.buffer(baos.toByteArray()))
        compressedBytesWritten += baos.size()
      }

      out.close()
      progress.current = bytesWritten

      resultFuture.await()
      return Metrics(fileSize, compressedBytesWritten)
    } finally {
      file.close()
    }
  }
}
