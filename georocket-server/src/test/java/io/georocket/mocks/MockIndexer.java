package io.georocket.mocks;

import io.georocket.constants.AddressConstants;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.rxjava.core.Vertx;
import rx.Subscription;

public class MockIndexer {


  /**
   * The number of hits per page
   */
  public static Long HITS_PER_PAGE = 50L;
  

  /**
   * The number of all hits to a given query
   */
  public static Long TOTAL_HITS = 80L;

  /**
   * The scrollId that gets returned from the indexer after the first query with a "null" scrollId given
   */
  public static final String FIRST_RETURNED_SCROLL_ID = "FIRST_SCROLL_ID";

  /**
   * The scrollId that gets returned when the FIRST_RETURNED_SCROLL_ID or "null" is given
   */
  public static final String INVALID_SCROLLID = "THIS_MOCK_INDEXER_ONLY_HAS_TWO_PAGES_THIS_SCROLLID_IS_INVALID";

  private static Subscription indexerQuerySubscription;

  public static void unsubscribeIndexer() {
    if (indexerQuerySubscription != null && !indexerQuerySubscription.isUnsubscribed()) {
      indexerQuerySubscription.unsubscribe();
    }
    indexerQuerySubscription = null;
  }

  public static void mockIndexerQuery(Vertx vertx) {
    indexerQuerySubscription = vertx.eventBus().<JsonObject>consumer(AddressConstants.INDEXER_QUERY).toObservable()
      .subscribe(msg -> {
        JsonArray hits = new JsonArray();

        String givenScrollId = msg.body().getString("scrollId");

        Long numReturnHits;
        String returnScrollId;
        if (givenScrollId == null) {
          numReturnHits = HITS_PER_PAGE;
          returnScrollId = FIRST_RETURNED_SCROLL_ID;
        } else if (givenScrollId.equals(FIRST_RETURNED_SCROLL_ID)) {
          numReturnHits = TOTAL_HITS - HITS_PER_PAGE;
          returnScrollId = INVALID_SCROLLID;
        } else {
          numReturnHits = 0L;
          returnScrollId = INVALID_SCROLLID;
        }

        for (int i = 0; i < numReturnHits; i++) {
          hits.add(
            new JsonObject()
              .put("mimeType", "application/geo+json")
              .put("id", "some_id")
              .put("start", 0)
              .put("end", MockStore.RETURNED_CHUNK.length())
              .put("parents", new JsonArray())
          );
        }

        msg.reply(
          new JsonObject()
            .put("totalHits", TOTAL_HITS)
            .put("scrollId", returnScrollId)
            .put("hits", hits)
        );
      });
  }

}
