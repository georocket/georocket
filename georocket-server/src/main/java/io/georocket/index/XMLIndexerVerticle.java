package io.georocket.index;

import com.google.common.collect.ImmutableList;
import io.georocket.index.xml.XMLIndexer;
import io.georocket.index.xml.XMLIndexerFactory;
import io.georocket.storage.ChunkMeta;
import io.georocket.storage.ChunkReadStream;
import io.georocket.storage.XMLChunkMeta;
import io.georocket.util.XMLParserOperator;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.rx.java.RxHelper;
import rx.Observable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;

/**
 * Background indexing of XML chunks added to the store
 * @author Michel Kraemer
 */
public class XMLIndexerVerticle extends ChunkIndexerVerticle {
  private static Logger log = LoggerFactory.getLogger(IndexerVerticle.class);

  @Override
  public void start(Future<Void> startFuture) {
    log.info("Launching XML indexer ...");
    super.start(startFuture);
  }

  @Override
  protected List<? extends IndexerFactory> createIndexerFactories() {
    // load and copy all indexer factories now and not lazily to avoid
    // concurrent modifications to the service loader's internal cache
    return ImmutableList.copyOf(ServiceLoader.load(XMLIndexerFactory.class));
  }
  
  @Override
  protected ChunkMeta makeChunkMeta(JsonObject json) {
    return new XMLChunkMeta(json);
  }
  
  @Override
  protected Observable<Map<String, Object>> chunkToDocument(ChunkReadStream chunk,
      String fallbackCRSString) {
    List<XMLIndexer> indexers = new ArrayList<>();
    indexerFactories.forEach(factory -> {
      XMLIndexer i = (XMLIndexer)factory.createIndexer();
      if (fallbackCRSString != null && i instanceof CRSAware) {
        ((CRSAware)i).setFallbackCRSString(fallbackCRSString);
      }
      indexers.add(i);
    });
    
    return RxHelper.toObservable(chunk)
      .lift(new XMLParserOperator())
      .doOnNext(e -> indexers.forEach(i -> i.onEvent(e)))
      .last() // "wait" until the whole chunk has been consumed
      .map(e -> {
        // create the Elasticsearch document
        Map<String, Object> doc = new HashMap<>();
        indexers.forEach(i -> doc.putAll(i.getResult()));
        return doc;
      });
  }
}
