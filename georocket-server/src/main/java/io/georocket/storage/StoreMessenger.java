package io.georocket.storage;

import static io.georocket.util.ThrowableHelper.throwableToCode;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.httpclient.util.DateParseException;
import org.apache.commons.httpclient.util.DateUtil;

import io.georocket.constants.AddressConstants;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.rxjava.core.AbstractVerticle;

/**
 * Class used to access the store methods over the event bus.
 * 
 * @author Yasmina Kammeyer
 *
 */
public class StoreMessenger extends AbstractVerticle {

  private static Logger log = LoggerFactory.getLogger(StoreMessenger.class);

  private Store store;

  @Override
  public void start(Future<Void> future) {
    registerAdd();
    registerGetOne();
    registerDelete();
    registerGet();
    registerGetSize();

    store = StoreFactory.createStore(getVertx());
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
          // XXX Send merge result
          // Merge everything into the result file
          log.error("Store.get called, but is not implemented yet.");
          msg.fail(-1, "Not supported.");
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
}
