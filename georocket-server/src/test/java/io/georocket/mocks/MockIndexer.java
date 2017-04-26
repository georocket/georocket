package io.georocket.mocks;

import io.georocket.constants.AddressConstants;
import io.georocket.storage.indexed.IndexedStore;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.rxjava.core.Vertx;
import rx.Subscription;

/**
 * Mocks an indexer
 * 
 * Consumes messages from {@link AddressConstants#INDEXER_QUERY} and returns hits.
 * The flow is the following:
 * If no scrollId is given in the request, returns {@link MockIndexer#HITS_PER_PAGE} items.
 * Also reply the scrollId {@link MockIndexer#FIRST_RETURNED_SCROLL_ID}.
 * 
 * If the scrollId {@link MockIndexer#FIRST_RETURNED_SCROLL_ID} is given, returns ({@link MockIndexer#TOTAL_HITS}- {@link MockIndexer#HITS_PER_PAGE}) items.
 * (This number IS smaller than {@link MockIndexer#HITS_PER_PAGE}!)
 * Also replies the scrollId {@link MockIndexer#INVALID_SCROLLID}.
 * 
 * If the scrollId {@link MockIndexer#INVALID_SCROLLID} is given, returns 0 items.
 * Also replies the scrollId {@link MockIndexer#INVALID_SCROLLID}.
 * 
 * So this MockIndexer simulates a query response that has 2 pages: one full and one not-full.
 * 
 * @author David Gengenbach
 */
public class MockIndexer {


  /**
   * The number of hits per page
   */
  public static Long HITS_PER_PAGE = new Long(IndexedStore.PAGE_SIZE);
  

  /**
   * The number of all hits to a given query
   */
  public static Long TOTAL_HITS = HITS_PER_PAGE + 1;

  /**
   * The scrollId that gets returned from the indexer after the first query with a "null" scrollId given
   */
  public static final String FIRST_RETURNED_SCROLL_ID = "FIRST_SCROLL_ID";

  /**
   * The scrollId that gets returned when the FIRST_RETURNED_SCROLL_ID or "null" is given
   */
  public static final String INVALID_SCROLLID = "THIS_MOCK_INDEXER_ONLY_HAS_TWO_PAGES_THIS_SCROLLID_IS_INVALID";

  private static Subscription indexerQuerySubscription;

  /**
   * Unsubscribe if an indexer is registered
   */
  public static void unsubscribeIndexer() {
    if (indexerQuerySubscription != null && !indexerQuerySubscription.isUnsubscribed()) {
      indexerQuerySubscription.unsubscribe();
    }
    indexerQuerySubscription = null;
  }

  /**
   * Start consuming {@link AddressConstants#INDEXER_QUERY} messages.
   * See the class comments to see the logic of the replied items.
   * 
   * Returns "valid" hits that correspond to the items that are returned from the {@link MockStore}.
   * 
   * @param vertx
   */
  public static void mockIndexerQuery(Vertx vertx) {
    indexerQuerySubscription = vertx.eventBus().<JsonObject>consumer(AddressConstants.INDEXER_QUERY).toObservable()
      .subscribe(msg -> {
        JsonArray hits = new JsonArray();

        String givenScrollId = msg.body().getString("scrollId");

        Long numberReturnHits;
        String returnScrollId;
        if (givenScrollId == null) {
          numberReturnHits = HITS_PER_PAGE;
          returnScrollId = FIRST_RETURNED_SCROLL_ID;
        } else if (givenScrollId.equals(FIRST_RETURNED_SCROLL_ID)) {
          numberReturnHits = TOTAL_HITS - HITS_PER_PAGE;
          returnScrollId = INVALID_SCROLLID;
        } else {
          numberReturnHits = 0L;
          returnScrollId = INVALID_SCROLLID;
        }

        for (int i = 0; i < numberReturnHits; i++) {
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
