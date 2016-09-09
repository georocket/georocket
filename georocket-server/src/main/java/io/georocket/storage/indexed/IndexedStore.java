package io.georocket.storage.indexed;

import java.util.ArrayDeque;
import java.util.Queue;

import io.georocket.constants.AddressConstants;
import io.georocket.storage.ChunkMeta;
import io.georocket.storage.IndexMeta;
import io.georocket.storage.Store;
import io.georocket.storage.StoreCursor;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.ReplyException;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.bson.types.ObjectId;

/**
 * An abstract base class for chunk stores that are backed by an indexer
 * @author Michel Kraemer
 */
public abstract class IndexedStore implements Store {
  private static final int PAGE_SIZE = 100;
  
  private final Vertx vertx;
  
  /**
   * Constructs the chunk store
   * @param vertx the Vert.x instance
   */
  public IndexedStore(Vertx vertx) {
    this.vertx = vertx;
  }
  
  @Override
  public void add(String chunk, ChunkMeta chunkMeta, String path,
      IndexMeta indexMeta, Handler<AsyncResult<Void>> handler) {
    doAddChunk(chunk, path, ar -> {
      if (ar.failed()) {
        handler.handle(Future.failedFuture(ar.cause()));
      } else {
        // start indexing
        JsonObject indexMsg = new JsonObject()
            .put("path", ar.result())
            .put("meta", chunkMeta.toJsonObject())
            .put("importId", indexMeta.getImportId())
            .put("filename", indexMeta.getFromFile())
            .put("importTime", indexMeta.getImportTimeStamp().getTime());

        if (indexMeta != null && indexMeta.getTags() != null) {
          indexMsg.put("tags", new JsonArray(indexMeta.getTags()));
        }
        if (indexMeta != null && indexMeta.getFallbackCRSString() != null) {
          indexMsg.put("fallbackCRSString", indexMeta.getFallbackCRSString());
        }
        vertx.eventBus().publish(AddressConstants.INDEXER_ADD, indexMsg);
        
        // tell sender that writing was successful
        handler.handle(Future.succeededFuture());
      }
    });
  }

  @Override
  public void delete(String search, String path, Handler<AsyncResult<Void>> handler) {
    new IndexedStoreCursor(vertx, PAGE_SIZE, search, path).start(ar -> {
      if (ar.failed()) {
        Throwable cause = ar.cause();
        if (cause instanceof ReplyException) {
          // Cast to get access to the failure code
          ReplyException ex = (ReplyException)cause;

          if (ex.failureCode() == 404) {
            handler.handle(Future.succeededFuture());
            return;
          }
        }
        handler.handle(Future.failedFuture(ar.cause()));
      } else {
        StoreCursor cursor = ar.result();
        Queue<String> paths = new ArrayDeque<>();
        doDelete(cursor, paths, handler);
      }
    });
  }

  @Override
  public void get(String search, String path, Handler<AsyncResult<StoreCursor>> handler) {
    new IndexedStoreCursor(vertx, PAGE_SIZE, search, path).start(handler);
  }
  
  /**
   * Iterate over a cursor and delete all returned chunks from the index
   * and from the store.
   * @param cursor the cursor to iterate over
   * @param paths an empty queue (used internally for recursion)
   * @param handler will be called when all chunks have been deleted
   */
  protected void doDelete(StoreCursor cursor, Queue<String> paths,
      Handler<AsyncResult<Void>> handler) {
    // handle response of bulk delete operation
    Handler<AsyncResult<Void>> handleBulk = bulkAr -> {
      if (bulkAr.failed()) {
        handler.handle(Future.failedFuture(bulkAr.cause()));
      } else {
        doDelete(cursor, paths, handler);
      }
    };
    
    // while cursor has items ...
    if (cursor.hasNext()) {
      cursor.next(ar -> {
        if (ar.failed()) {
          handler.handle(Future.failedFuture(ar.cause()));
        } else {
          // add item to queue
          paths.add(cursor.getChunkPath());
          
          if (paths.size() >= PAGE_SIZE) {
            // if there are enough items in the queue, bulk delete them
            doDeleteBulk(paths, handleBulk);
          } else {
            // otherwise proceed with next item from cursor
            doDelete(cursor, paths, handler);
          }
        }
      });
    } else {
      // cursor does not return any more items
      if (!paths.isEmpty()) {
        // bulk delete the remaining ones
        doDeleteBulk(paths, handleBulk);
      } else {
        // end operation
        handler.handle(Future.succeededFuture());
      }
    }
  }
  
  /**
   * Delete all chunks with the given paths from the index and from the store.
   * Remove all items from the given queue.
   * @param paths the queue of paths of chunks to delete (will be empty when
   * the operation has finished)
   * @param handler will be called when the operation has finished
   */
  protected void doDeleteBulk(Queue<String> paths, Handler<AsyncResult<Void>> handler) {
    // delete from index first so the chunks cannot be found anymore
    JsonArray jsonPaths = new JsonArray();
    paths.forEach(jsonPaths::add);
    JsonObject indexMsg = new JsonObject()
        .put("paths", jsonPaths);
    vertx.eventBus().send(AddressConstants.INDEXER_DELETE, indexMsg, ar -> {
      if (ar.failed()) {
        handler.handle(Future.failedFuture(ar.cause()));
      } else {
        // now delete all chunks from file system and clear the queue
        doDeleteChunks(paths, handler);
      }
    });
  }

  /**
   * Generate or get an unique identifier for a given chunk. This
   * method generates an identifier independently of the chunk itself.
   * Inheritance classes may override this to generate identifiers,
   * that are linked to the chunk they belong to.
   * @param chunk chunk to generate the id for
   * @return chunk identifier
   */
  protected String generateChunkId(String chunk) {
    return new ObjectId().toString();
  }
  
  /**
   * Add a chunk to the store
   * @param chunk the chunk to add
   * @param path the chunk's destination path
   * @param handler will be called when the operation has finished
   */
  protected abstract void doAddChunk(String chunk, String path, Handler<AsyncResult<String>> handler);
  
  /**
   * Delete all chunks with the given paths from the store. Remove one item
   * from the given queue, delete the chunk, and then call recursively until
   * all items have been removed from the queue. Finally, call the given handler.
   * @param paths the queue of paths of chunks to delete (will be empty when
   * the operation has finished)
   * @param handler will be called when the operation has finished
   */
  protected abstract void doDeleteChunks(Queue<String> paths, Handler<AsyncResult<Void>> handler);
}
