package io.georocket.cli

import de.undercouch.underline.InputReader
import io.georocket.index.Index
import io.georocket.index.IndexerFactory
import io.georocket.index.MetaIndexerFactory
import io.georocket.index.mongodb.MongoDBIndex
import io.georocket.query.DefaultQueryCompiler
import io.georocket.storage.Store
import io.georocket.storage.StoreFactory
import io.georocket.util.PathUtils
import io.vertx.core.buffer.Buffer
import io.vertx.core.json.JsonObject
import io.vertx.core.streams.WriteStream

/**
 * A command that deals with the [Store] and the [Index]
 */
abstract class DataCommand : GeoRocketCommand() {
  protected fun compileQuery(search: String?, layer: String?): JsonObject {
    return DefaultQueryCompiler(MetaIndexerFactory.ALL + IndexerFactory.ALL)
      .compileQuery(search ?: "", PathUtils.addLeadingSlash(layer ?: ""))
  }

  override suspend fun doRun(remainingArgs: Array<String>, reader: InputReader,
      out: WriteStream<Buffer>): Int {
    val store = StoreFactory.createStore(vertx)
    return try {
      val index = MongoDBIndex.create(vertx)
      try {
        doRun(remainingArgs, reader, out, store, index)
      } finally {
        index.close()
      }
    } finally {
      store.close()
    }
  }

  abstract suspend fun doRun(remainingArgs: Array<String>, reader: InputReader,
    out: WriteStream<Buffer>, store: Store, index: Index): Int
}