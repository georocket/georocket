package io.georocket.index.xml;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jooq.lambda.Seq;

import io.georocket.index.CRSAware;
import io.georocket.index.ChunkMapper;
import io.georocket.index.IndexerFactory;
import io.georocket.storage.ChunkMeta;
import io.georocket.storage.ChunkReadStream;
import io.georocket.storage.XMLChunkMeta;
import io.georocket.util.XMLParserOperator;
import io.vertx.core.json.JsonObject;
import io.vertx.rx.java.RxHelper;
import rx.Observable;

/**
 * Maps XML chunks to Elasticsearch documents and vice-versa
 * @author Michel Kraemer
 */
public class XMLChunkMapper implements ChunkMapper {
  private List<XMLIndexerFactory> indexerFactories;

  /**
   * Construct the mapper
   * @param indexerFactories a list of all indexer factory services
   */
  public XMLChunkMapper(Collection<? extends IndexerFactory> indexerFactories) {
    this.indexerFactories = Seq.seq(indexerFactories)
      .filter(f -> f instanceof XMLIndexerFactory)
      .cast(XMLIndexerFactory.class)
      .toList();
  }

  @Override
  public ChunkMeta makeChunkMeta(JsonObject json) {
    return new XMLChunkMeta(json);
  }

  @Override
  public Observable<Map<String, Object>> chunkToDocument(
      ChunkReadStream chunk, String fallbackCRSString) {
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
