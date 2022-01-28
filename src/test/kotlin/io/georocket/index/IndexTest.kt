package io.georocket.index

import io.georocket.coVerify
import io.georocket.query.All
import io.georocket.storage.ChunkMeta
import io.vertx.core.Vertx
import io.vertx.junit5.VertxExtension
import io.vertx.junit5.VertxTestContext
import io.vertx.kotlin.core.json.jsonObjectOf
import io.vertx.kotlin.coroutines.dispatcher
import kotlinx.coroutines.*
import org.junit.jupiter.api.Test
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.extension.ExtendWith

/**
 * Abstract test implementation for a [Index]
 */
@ExtendWith(VertxExtension::class)
abstract class IndexTest {
  abstract suspend fun createIndex(vertx: Vertx): Index

  abstract suspend fun prepareTestData(vertx: Vertx, docs: List<Index.AddManyParam>)

  @Test
  fun testGetPaginatedMeta(ctx: VertxTestContext, vertx: Vertx) {
    CoroutineScope(vertx.dispatcher()).launch {
      ctx.coVerify {

        // prepare index
        val paths = setOf("document1", "document2", "document3", "document4", "document5")
        prepareTestData(vertx, paths.map { path ->
          Index.AddManyParam(path, jsonObjectOf(), ChunkMeta("application/json"))
        }.toList())
        val index = createIndex(vertx)

        // get first 3 pages
        val query = All
        val page1 = index.getPaginatedMeta(query, 3, null)
        val page2 = index.getPaginatedMeta(query, 3, page1.scrollId)
        val page3 = index.getPaginatedMeta(query, 3, page2.scrollId)

        // check results
        assertThat(page1.items.size).isEqualTo(3)
        assertThat(page2.items.size).isEqualTo(2)
        assertThat(page3.items.size).isEqualTo(0)
        val returnedPaths = listOf(page1, page2, page3).flatMap { it.items }.map { (path, _) -> path }.toSet()
        assertThat(returnedPaths).isEqualTo(paths)
        index.close()
      }
      ctx.completeNow()
    }
  }
}
