package io.georocket.index.mongodb

import io.georocket.index.Index
import io.georocket.index.IndexTest
import io.georocket.storage.mongodb.MongoDBTestConnector
import io.vertx.core.Vertx
import org.junit.jupiter.api.AfterAll

class MongoDBIndexTest : IndexTest() {

    companion object {
        val mongoConnector by lazy { MongoDBTestConnector() }

        @AfterAll
        @JvmStatic
        fun tearDownClass() {
            mongoConnector.stop()
        }

        private val COLLECTIONS = listOf("documents", "chunkMeta", "ogcapifeatures.collections")
    }

    override suspend fun createIndex(vertx: Vertx): Index =
        MongoDBIndex.create(vertx, mongoConnector.connectionString.connectionString, null)


    override suspend fun prepareTestData(vertx: Vertx, docs: List<Index.AddManyParam>) {

        // reset database
        val cs = mongoConnector.connectionString
        val client = SharedMongoClient.create(cs)
        val db = client.getDatabase(cs.database)
        COLLECTIONS.forEach {
            db.getCollection(it).drop()
        }
        client.close()

        // insert test data
        val index = createIndex(vertx)
        index.addMany(docs)
        index.close()
    }

}