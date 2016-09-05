package io.georocket.storage;

import static io.georocket.util.ThrowableHelper.throwableToCode;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.httpclient.util.DateParseException;
import org.apache.commons.httpclient.util.DateUtil;
import org.apache.commons.lang3.tuple.Pair;

import io.georocket.constants.AddressConstants;
import io.georocket.output.Merger;
import io.georocket.util.io.BufferWriteStream;
import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.streams.WriteStream;
import io.vertx.rxjava.core.AbstractVerticle;
import rx.Observable;

/**
 * Class used to access the store methods over the event bus.
 * 
 * Merger code from {@link io.georocket.http.StoreEndpoint}.
 * 
 * @author Yasmina Kammeyer
 *
 */
public abstract class StoreMessenger<M extends Merger> extends AbstractVerticle {

  private static Logger log = LoggerFactory.getLogger(StoreMessenger.class);

  private RxStore store;

  @Override
  public void start(Future<Void> future) {
    // create a unique message address
    // 
    
    registerAdd();
    registerGetOne();
    registerDelete();
    registerGet();
    registerGetSize();

    publishService();
    
    store = new RxStore(StoreFactory.createStore(getVertx()));
  }

  private void publishService() {
    // TODO Auto-generated method stub
    
  }

  /**
   * The message contains parameter to invoke the {@link Store#add} method.
   * 
   */
  private void registerAdd() {
    vertx.eventBus().<JsonObject> consumer(AddressConstants.STORE_ADD)
    .toObservable()
    .subscribe(msg -> {
      JsonObject body = msg.body();
          ChunkMeta chunkMeta = new ChunkMeta(body.getJsonObject("chunkMeta"));
          IndexMeta indexMeta;
          try {
            indexMeta = createIndexMetaFromJson(body.getJsonObject("indexMeta"));

            store.add(body.getString("chunk"), chunkMeta, body.getString("path"), indexMeta, handler -> {
              if (handler.succeeded()) {
                msg.reply(Future.succeededFuture());
              } else {
                log.error("Could not add chunks", handler.cause());
                msg.fail(throwableToCode(handler.cause()), handler.cause().getMessage());
              }
            });
          } catch (DateParseException e) {
            log.error("Could not create Index Meta, because Date entry was not valid.", e);
            msg.fail(throwableToCode(e), e.getMessage());
          }
    });
  }
  
  /**
   * Used to parse a given JSON object containing the index meta values
   * 
   * @param object The JSON Object to parse
   * @return The created IndexMeta
   * @throws DateParseException If date value was not a valid date string
   */
  private IndexMeta createIndexMetaFromJson(JsonObject object) throws DateParseException {
    List<String> tags = new ArrayList<String>();
    object.getJsonArray("tags").forEach(e -> {
      tags.add(e.toString());
    });
    String fallbackCRSString = object.getString("fallbackCRSString");
    String importId = object.getString("importId");
    String fromFile = object.getString("fromFile");
    Date importTimeStamp = DateUtil.parseDate(object.getString("importTimeStamp"));
    
    return new IndexMeta(importId, fromFile, importTimeStamp, tags, fallbackCRSString);
  }

  /**
   * The message contains parameter to invoke the {@link Store#getOne} method.
   */
  private void registerGetOne() {
    vertx.eventBus().<JsonObject> consumer(AddressConstants.STORE_GET_ONE)
    .toObservable()
    .subscribe(msg -> {
      store.getOne(msg.body().getString("path"), handler -> {
        if (handler.succeeded()) {
          ChunkReadStream crs = handler.result();
          Buffer chunk = Buffer.buffer();
          crs.handler(buf -> {
            chunk.appendBuffer(buf);
          });
          crs.endHandler(v -> {
            msg.reply(chunk);
          });
        } else {
          log.error("Could not get chunk", handler.cause());
          msg.fail(throwableToCode(handler.cause()), handler.cause().getMessage());
        }
      });
    });
  }

  /**
   * The message contains parameter to invoke the {@link Store#delete} method.
   */
  private void registerDelete() {
    vertx.eventBus().<JsonObject> consumer(AddressConstants.STORE_DELETE)
    .toObservable()
    .subscribe(msg -> {
      JsonObject body = msg.body();

      store.delete(body.getString("search"), body.getString("path"), handler -> {
        if (handler.succeeded()) {
          msg.reply(Future.succeededFuture());
        } else {
          log.error("Could not delete chunks", handler.cause());
          msg.fail(throwableToCode(handler.cause()), handler.cause().getMessage());
        }
      });
    });
  }

  private void registerGet() {
    vertx.eventBus().<JsonObject> consumer(AddressConstants.STORE_GET)
    .toObservable()
    .subscribe(msg -> {
      // get path and search parameter
      JsonObject body = msg.body();
      String search = body.getString("search");
      String path = body.getString("path");
      
      WriteStream<Buffer> bufferWriteStream = new BufferWriteStream();
      // get specific merger
      Merger merger = getMerger();
      // initialize merger
      initializeMerger(merger, search, path)
      // do the merge (write all chunks to stream)
        .flatMap(v -> doMerge(merger, search, path, bufferWriteStream))
        .subscribe(v -> {
          bufferWriteStream.end();
          // send buffer as reply
          msg.reply(((BufferWriteStream)bufferWriteStream).getBuffer());
        }, err -> {
          if (!(err instanceof FileNotFoundException)) {
            log.error("Could not perform query", err);
          }
          msg.fail(-1, "Could not perform query.");
        });
      
    });
  }

  private void registerGetSize() {
    vertx.eventBus().<JsonObject> consumer(AddressConstants.STORE_GET_SIZE)
    .toObservable()
    .subscribe(msg -> {
      store.getSize(handler -> {
        if (handler.succeeded()) {
          msg.reply(handler.result());
        } else {
          log.error("Could not get size", handler.cause());
          msg.fail(throwableToCode(handler.cause()), handler.cause().getMessage());
        }
      });
    });
  }

  /**
   * Initialize the given merger. Perform a search using the given search string
   * and pass all chunk metadata retrieved to the merger.
   * @param merger the merger to initialize
   * @param search the search query
   * @param path the path where to perform the search
   * @return an observable that will emit exactly one item when the merger
   * has been initialized with all results
   */
  private Observable<Void> initializeMerger(Merger merger, String search,
      String path) {
    return store.getObservable(search, path)
      .map(RxStoreCursor::new)
      .flatMap(RxStoreCursor::toObservable)
      .map(Pair::getLeft)
      .cast(XMLChunkMeta.class)
      .flatMap(merger::initObservable)
      .defaultIfEmpty(null)
      .last();
  }
  
  /**
   * Perform a search and merge all retrieved chunks using the given merger
   * @param merger the merger
   * @param search the search query
   * @param path the path where to perform the search
   * @param out a write stream to write the merged chunks to
   * @return an observable that will emit one item when all chunks have been merged
   */
  private Observable<Void> doMerge(Merger merger, String search, String path,
      WriteStream<Buffer> out) {
    return store.getObservable(search, path)
      .map(RxStoreCursor::new)
      .flatMap(RxStoreCursor::toObservable)
      .flatMap(p -> store.getOneObservable(p.getRight())
        // TODO handle cast to XMLChunkMeta according to mime type
        .flatMap(crs -> merger.mergeObservable(crs, (XMLChunkMeta)p.getLeft(), out)
          .map(v -> Pair.of(1L, 0L)) // left: count, right: not_accepted
          .onErrorResumeNext(t -> {
            if (t instanceof IllegalStateException) {
              // Chunk cannot be merged. maybe it's a new one that has
              // been added after the Merger was initialized. Just
              // ignore it, but emit a warning later
              return Observable.just(Pair.of(0L, 1L));
            }
            return Observable.error(t);
          })
          .doOnTerminate(() -> {
            // don't forget to close the chunk!
            crs.close();
          })), 1 /* write only one chunk concurrently to the output stream */)
      .defaultIfEmpty(Pair.of(0L, 0L))
      .reduce((p1, p2) -> Pair.of(p1.getLeft() + p2.getLeft(),
          p1.getRight() + p2.getRight()))
      .flatMap(p -> {
        long count = p.getLeft();
        long notaccepted = p.getRight();
        if (notaccepted > 0) {
          log.warn("Could not merge " + notaccepted + " chunks "
              + "because the merger did not accept them. Most likely "
              + "these are new chunks that were added while the "
              + "merge was in progress. If this worries you, just "
              + "repeat the request.");
        }
        if (count > 0) {
          merger.finishMerge(out);
          return Observable.just(null);
        } else {
          return Observable.error(new FileNotFoundException("Not Found"));
        }
      });
  }
  
  /**
   * Override this method to provide the specific Merger to use.
   * 
   * @return The merger to use or <code>null</code> if merging not supported
   */
  protected abstract M getMerger();
}
