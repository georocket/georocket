package io.georocket.output.xml;

import io.georocket.storage.XMLChunkMeta;
import rx.Completable;
import rx.Single;

/**
 * Merge chunks whose root XML elements are all equal
 * @author Michel Kraemer
 */
public class AllSameStrategy extends AbstractMergeStrategy {
  @Override
  public Single<Boolean> canMerge(XMLChunkMeta meta) {
    if (getParents() == null || getParents().equals(meta.getParents())) {
      return Single.just(Boolean.TRUE);
    } else {
      return Single.just(Boolean.FALSE);
    }
  }

  @Override
  protected Completable mergeParents(XMLChunkMeta meta) {
    if (getParents() == null) {
      setParents(meta.getParents());
    }
    return Completable.complete();
  }
}
