package io.georocket.index.generic;

import static io.georocket.query.ElasticsearchQueryHelper.geoShapeQuery;
import static io.georocket.query.ElasticsearchQueryHelper.shape;

import java.util.Arrays;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.collect.ImmutableMap;

import io.georocket.constants.ConfigConstants;
import io.georocket.index.IndexerFactory;
import io.georocket.util.CoordinateTransformer;
import io.vertx.core.Context;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.geotools.referencing.CRS;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.TransformException;

/**
 * Base class for factories creating indexers that manage bounding boxes
 * @author Michel Kraemer
 */
public abstract class BoundingBoxIndexerFactory implements IndexerFactory {
  private static final String FLOAT_REGEX = "[-+]?[0-9]*\\.?[0-9]+([eE][-+]?[0-9]+)?";
  private static final String COMMA_REGEX = "\\s*,\\s*";
  private static final String CODE_PREFIX = "([a-zA-Z]+:\\d+:)?";
  private static final String BBOX_REGEX = CODE_PREFIX + FLOAT_REGEX + COMMA_REGEX + FLOAT_REGEX +
    COMMA_REGEX + FLOAT_REGEX + COMMA_REGEX + FLOAT_REGEX;
  private static final Pattern BBOX_PATTERN = Pattern.compile(BBOX_REGEX);
  private String defaultCrs;

  /**
   * Default constructor
   */
  public BoundingBoxIndexerFactory() {
    Context ctx = Vertx.currentContext();
    if (ctx != null) {
      JsonObject config = ctx.config();
      if (config != null) {
        setDefaultCrs(config.getString(ConfigConstants.QUERY_DEFAULT_CRS));
      }
    }
  }

  /**
   * Set the default CRS which is used to transform query bounding box coordinates
   * to WGS84 coordinates
   * @param defaultCrs the CRS string (see {@link CoordinateTransformer#decode(String)}).
   */
  public void setDefaultCrs(String defaultCrs) {
    this.defaultCrs = defaultCrs;
  }

  @Override
  public Map<String, Object> getMapping() {
    String precisionKey;
    Object precisionValue = null;

    // check if we have a current Vert.x context from which we can get the config
    Context ctx = Vertx.currentContext();
    if (ctx != null) {
      JsonObject config = ctx.config();
      if (config != null) {
        precisionValue = config.getString(ConfigConstants.INDEX_SPATIAL_PRECISION);
      }
    }

    if (precisionValue == null) {
      // use the maximum number of tree levels to achieve highest precision
      // see org.apache.lucene.spatial.prefix.tree.PackedQuadPrefixTree.MAX_LEVELS_POSSIBLE
      precisionKey = "tree_levels";
      precisionValue = 29;
    } else {
      precisionKey = "precision";
    }

    return ImmutableMap.of("properties", ImmutableMap.of("bbox", ImmutableMap.of(
        "type", "geo_shape",
        
        // for a discussion on the tree type to use see
        // https://github.com/elastic/elasticsearch/issues/14181
        
        // quadtree uses less memory and seems to be a lot faster than geohash
        // see http://tech.taskrabbit.com/blog/2015/06/09/elasticsearch-geohash-vs-geotree/
        "tree", "quadtree",
        
        precisionKey, precisionValue
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
    CoordinateReferenceSystem crs = null;
    String crsCode = null;
    String co;
    int index = search.lastIndexOf(':');

    if (index > 0) {
      crsCode = search.substring(0, index);
      co = search.substring(index + 1);
    } else {
      co = search;
    }

    double[] points = Arrays.stream(co.split(","))
      .map(String::trim)
      .mapToDouble(Double::parseDouble)
      .toArray();

    if (crsCode != null) {
      try {
        crs = CRS.decode(crsCode);
      } catch (FactoryException e) {
        throw new RuntimeException(
          String.format("CRS %s could not be parsed: %s",
            crsCode, e.getMessage()), e);
      }
    } else if (defaultCrs != null) {
      try {
        crs = CoordinateTransformer.decode(defaultCrs);
      } catch (FactoryException e) {
        throw new RuntimeException(
          String.format("Default CRS %s could not be parsed: %s",
            defaultCrs, e.getMessage()), e);
      }
    }

    if (crs != null) {
      try {
        CoordinateTransformer transformer = new CoordinateTransformer(crs);
        points = transformer.transform(points, -1);
      } catch (FactoryException e) {
        throw new RuntimeException(String.format("CRS %s could not be parsed: %s",
          crsCode, e.getMessage()), e);
      } catch (TransformException e) {
        throw new RuntimeException(String.format("Coordinates %s could not be "
          + "transformed to %s: %s", co, crsCode, e.getMessage()), e);
      }
    }

    double minX = points[0];
    double minY = points[1];
    double maxX = points[2];
    double maxY = points[3];
    JsonArray coordinates = new JsonArray();
    coordinates.add(new JsonArray().add(minX).add(maxY));
    coordinates.add(new JsonArray().add(maxX).add(minY));
    return geoShapeQuery("bbox", shape("envelope", coordinates), "intersects");
  }
}
