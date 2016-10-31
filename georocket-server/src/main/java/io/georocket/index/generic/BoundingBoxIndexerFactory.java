package io.georocket.index.generic;

import static io.georocket.query.ElasticsearchQueryHelper.geoShapeQuery;
import static io.georocket.query.ElasticsearchQueryHelper.shape;

import java.util.Iterator;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;

import io.georocket.index.IndexerFactory;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

/**
 * Base class for factories creating indexers that manage bounding boxes
 * @author Michel Kraemer
 */
public abstract class BoundingBoxIndexerFactory implements IndexerFactory {
  private static final String FLOAT_REGEX = "[-+]?[0-9]*\\.?[0-9]+([eE][-+]?[0-9]+)?";
  private static final String COMMA_REGEX = "\\s*,\\s*";
  private static final String BBOX_REGEX = FLOAT_REGEX + COMMA_REGEX + FLOAT_REGEX +
      COMMA_REGEX + FLOAT_REGEX + COMMA_REGEX + FLOAT_REGEX;
  private static final Pattern BBOX_PATTERN = Pattern.compile(BBOX_REGEX);

  @Override
  public Map<String, Object> getMapping() {
    return ImmutableMap.of("properties", ImmutableMap.of("bbox", ImmutableMap.of(
        "type", "geo_shape",
        
        // for a discussion on the tree type to use see
        // https://github.com/elastic/elasticsearch/issues/14181
        
        // quadtree uses less memory and seems to be a lot faster than geohash
        // see http://tech.taskrabbit.com/blog/2015/06/09/elasticsearch-geohash-vs-geotree/
        "tree", "quadtree",
        
        // use the maximum number of tree levels to achieve highest precision
        // see org.apache.lucene.spatial.prefix.tree.PackedQuadPrefixTree.MAX_LEVELS_POSSIBLE
        "tree_levels", "29"
        
        // TODO Setting tree_levels to 29 might be too slow for very large data
        // sets. It might be better to reduce the precision by setting the
        // 'precision' property to "1m" or "10m" or something similar. This
        // should be configurable in GeoRocket's configuration file.
        // "tree_levels": "29" could be the default if no 'precision' is set in
        // the config file.
    )));
  }

  @Override
  public MatchPriority getQueryPriority(String search) {
    Matcher bboxMatcher = BBOX_PATTERN.matcher(search);
    if (bboxMatcher.matches()) {
      return MatchPriority.ONLY;
    }
    return MatchPriority.NONE;
  }

  @Override
  public JsonObject compileQuery(String search) {
    Iterable<String> coords = Splitter.on(',').trimResults().split(search);
    Iterator<String> coordsIter = coords.iterator();
    double minX = Double.parseDouble(coordsIter.next());
    double minY = Double.parseDouble(coordsIter.next());
    double maxX = Double.parseDouble(coordsIter.next());
    double maxY = Double.parseDouble(coordsIter.next());
    JsonArray coordinates = new JsonArray();
    coordinates.add(new JsonArray().add(minX).add(maxY));
    coordinates.add(new JsonArray().add(maxX).add(minY));
    return geoShapeQuery("bbox", shape("envelope", coordinates), "intersects");
  }
}
