package io.georocket.output.xml

import io.georocket.storage.XMLChunkMeta
import rx.Completable
import rx.Single

/**
 * Merge chunks whose root XML elements are all equal
 * @author Michel Kraemer
 */
class AllSameStrategy : AbstractMergeStrategy() {
  override fun canMerge(meta: XMLChunkMeta): Single<Boolean> {
    return Single.defer {
      if (parents == null || parents == meta.parents) {
        Single.just(true)
      } else {
        Single.just(false)
      }
    }
  }

  override fun mergeParents(chunkMetadata: XMLChunkMeta): Completable {
    if (parents == null) {
      parents = chunkMetadata.parents
    }
    return Completable.complete()
  }
}
