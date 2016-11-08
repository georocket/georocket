package io.georocket.output.xml;

import io.georocket.storage.XMLChunkMeta;
import rx.Observable;

/**
 * Merge chunks whose root XML elements are all equal
 * @author Michel Kraemer
 */
public class AllSameStrategy extends AbstractMergeStrategy {
  @Override
  public Observable<Boolean> canMerge(XMLChunkMeta meta) {
    if (getParents() == null || getParents().equals(meta.getParents())) {
      return Observable.just(Boolean.TRUE);
    } else {
      return Observable.just(Boolean.FALSE);
    }
  }

  @Override
  protected Observable<Void> mergeParents(XMLChunkMeta meta) {
    if (getParents() == null) {
      setParents(meta.getParents());
    }
    return Observable.just(null);
  }
}
