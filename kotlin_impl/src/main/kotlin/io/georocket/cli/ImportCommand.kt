package io.georocket.cli

import de.undercouch.underline.InputReader
import de.undercouch.underline.Option.ArgumentType
import de.undercouch.underline.OptionDesc
import de.undercouch.underline.UnknownAttributes
import io.georocket.ImporterVerticle
import io.georocket.constants.AddressConstants
import io.georocket.index.PropertiesParser
import io.georocket.index.TagsParser
import io.georocket.tasks.ImportingTask
import io.georocket.tasks.TaskRegistry
import io.georocket.util.MimeTypeUtils
import io.georocket.util.SizeFormat
import io.georocket.util.formatUntilNow
import io.vertx.core.buffer.Buffer
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.core.streams.WriteStream
import io.vertx.kotlin.core.deploymentOptionsOf
import io.vertx.kotlin.coroutines.await
import kotlinx.coroutines.delay
import org.antlr.v4.runtime.misc.ParseCancellationException
import org.apache.commons.io.FilenameUtils
import org.apache.commons.lang3.SystemUtils
import org.apache.tools.ant.Project
import org.apache.tools.ant.types.FileSet
import org.bson.types.ObjectId
import org.fusesource.jansi.AnsiConsole
import org.slf4j.LoggerFactory
import java.io.File
import java.io.IOException
import java.nio.file.Paths

/**
 * Import one or more files into GeoRocket
 */
class ImportCommand : GeoRocketCommand() {
  companion object {
    private val log = LoggerFactory.getLogger(ImportCommand::class.java)
  }

  override val usageName = "import"
  override val usageDescription = "Import one or more files into GeoRocket"

  private data class Metrics(val bytesImported: Long)

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

  @set:OptionDesc(longName = "tags", shortName = "t",
    description = "comma-separated list of tags to attach to the file(s)",
    argumentName = "TAGS", argumentType = ArgumentType.STRING)
  var tags: String? = null

  @set:OptionDesc(longName = "properties", shortName = "props",
    description = "comma-separated list of properties (key:value) to attach to the file(s)",
    argumentName = "PROPERTIES", argumentType = ArgumentType.STRING)
  var properties: String? = null

  override fun checkArguments(): Boolean {
    if (patterns.isEmpty()) {
      error("no file pattern given. provide at least one file to import.")
      return false
    }
    if (tags != null) {
      try {
        TagsParser.parse(tags)
      } catch (e: ParseCancellationException) {
        error("Invalid tag syntax: ${e.message}")
        return false
      }
    }
    if (properties != null) {
      try {
        PropertiesParser.parse(properties)
      } catch (e: ParseCancellationException) {
        error("Invalid property syntax: ${e.message}")
        return false
      }
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

  override suspend fun doRun(remainingArgs: Array<String>, reader: InputReader,
      out: WriteStream<Buffer>): Int {
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
      val metrics = doImport(files)

      var m = "file"
      if (files.size > 1) {
        m += "s"
      }
      println("Successfully imported ${files.size} $m")
      println("  Total time:         ${start.formatUntilNow()}")
      println("  Total data size:    ${SizeFormat.format(metrics.bytesImported)}")

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
      val props = fs.props(path).await()
      Pair(path, props.size())
    }
  }

  /**
   * Import files using a HTTP client and finally call a handler
   * @param files the files to import
   * @return an observable that will emit metrics when all files have been imported
   */
  private suspend fun doImport(files: List<String>): Metrics {
    AnsiConsole.systemInstall()

    // launch importer verticle
    val importerVerticleId = vertx.deployVerticle(ImporterVerticle(),
      deploymentOptionsOf(config = config)).await()
    try {
      ImportProgressRenderer(vertx).use { progress ->
        progress.totalFiles = files.size

        val filesWithSizes = getFileSizes(files)
        val totalSize = filesWithSizes.sumOf { it.second }
        progress.totalSize = totalSize

        var bytesImported = 0L
        for (file in filesWithSizes.withIndex()) {
          val path = file.value.first
          val size = file.value.second
          val index = file.index

          progress.startNewFile(Paths.get(path).fileName.toString())
          progress.index = index
          progress.size = size

          val m = importFile(path, size, progress)
          bytesImported += m.bytesImported
        }

        return Metrics(bytesImported)
      }
    } finally {
      vertx.undeploy(importerVerticleId).await()
      AnsiConsole.systemUninstall()
    }
  }

  /**
   * Try to detect the content type of a file with the given [filepath].
   */
  private suspend fun detectContentType(filepath: String): String {
    return vertx.executeBlocking<String> { f ->
      try {
        var mimeType = MimeTypeUtils.detect(File(filepath))
        if (mimeType == null) {
          log.warn("Could not detect file type of $filepath. Falling back to " +
              "application/octet-stream.")
          mimeType = "application/octet-stream"
        }
        f.complete(mimeType)
      } catch (e: IOException) {
        f.fail(e)
      }
    }.await()
  }

  /**
   * Upload a file to GeoRocket
   * @param path the path to the file to import
   * @param fileSize the size of the file
   * @param progress a renderer that display the progress on the terminal
   * @return a metrics object
   */
  private suspend fun importFile(path: String, fileSize: Long,
      progress: ImportProgressRenderer): Metrics {
    val detectedContentType = detectContentType(path).also {
      log.info("Guessed mime type '$it'.")
    }
    val correlationId = ObjectId().toString()

    val msg = JsonObject()
      .put("filepath", path)
      .put("layer", layer)
      .put("contentType", detectedContentType)
      .put("correlationId", correlationId)

    if (tags != null) {
      msg.put("tags", JsonArray(TagsParser.parse(tags)))
    }

    if (properties != null) {
      msg.put("properties", JsonObject(PropertiesParser.parse(properties)))
    }

    if (fallbackCRS != null) {
      msg.put("fallbackCRSString", fallbackCRS)
    }

    // run importer
    val taskId = vertx.eventBus().request<String>(AddressConstants.IMPORTER_IMPORT, msg).await().body()

    while (true) {
      val t = (TaskRegistry.getById(taskId) ?: break) as ImportingTask
      progress.current = t.bytesProcessed
      if (t.endTime != null) {
        break
      }
      delay(100)
    }

    progress.current = fileSize
    return Metrics(fileSize)
  }
}
