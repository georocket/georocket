package io.georocket.cli

import de.undercouch.underline.InputReader
import io.georocket.GeoRocket
import io.georocket.util.SizeFormat
import io.vertx.core.DeploymentOptions
import io.vertx.core.Promise
import io.vertx.core.logging.LoggerFactory
import io.vertx.kotlin.core.deployVerticleAwait
import io.vertx.kotlin.coroutines.await
import java.io.PrintWriter
import java.lang.management.ManagementFactory

/**
 * Run GeoRocket in server mode
 */
class ServerCommand : GeoRocketCommand() {
  companion object {
    private val log = LoggerFactory.getLogger(ServerCommand::class.java)
  }

  override val usageName = "server"
  override val usageDescription = "Run GeoRocket in server mode"

  override suspend fun doRun(remainingArgs: Array<String>, reader: InputReader,
      writer: PrintWriter): Int {
    // print banner
    val banner = GeoRocket::class.java.getResource("georocket_banner.txt")!!.readText()
    println(banner)

    // log memory info
    val memoryMXBean = ManagementFactory.getMemoryMXBean()
    val memoryInit = memoryMXBean.heapMemoryUsage.init
    val memoryMax = memoryMXBean.heapMemoryUsage.max
    log.info("Initial heap size: ${SizeFormat.format(memoryInit)}, " +
        "max heap size: ${SizeFormat.format(memoryMax)}")

    // deploy main verticle
    val shutdownPromise = Promise.promise<Unit>()
    try {
      val options = DeploymentOptions().setConfig(config)
      vertx.deployVerticleAwait(GeoRocket(shutdownPromise), options)
    } catch (t: Throwable) {
      log.fatal("Could not deploy GeoRocket")
      t.printStackTrace()
      return 1
    }

    // wait until GeoRocket verticle has shut down
    shutdownPromise.future().await()

    return 0
  }
}
