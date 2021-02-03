package io.georocket.storage.h2

import org.h2.mvstore.MVStore
import java.io.File
import java.util.concurrent.atomic.AtomicReference

/**
 * Holds a shared instance of [MVStore]
 * @author Michel Kraemer
 */
class SharedMVStoreHolder(private val path: String, private val compress: Boolean) {
  /**
   * The underlying H2 MVStore
   */
  private val sharedMVStore = AtomicReference<MVStore>()

  /**
   * Get or create the shared instance
   */
  val mvStore: MVStore get() {
    var result = sharedMVStore.get()
    if (result == null) {
      synchronized(sharedMVStore) {
        val dir = File(path).parentFile
        if (!dir.exists()) {
          dir.mkdirs()
        }

        var builder = MVStore.Builder().fileName(path)
        if (compress) {
          builder = builder.compress()
        }
        result = builder.open()
        sharedMVStore.set(result)
      }
    }
    return result
  }

  /**
   * Close the shared instance and release all resources
   */
  fun close() {
    val s = sharedMVStore.getAndSet(null)
    s?.close()
  }
}
