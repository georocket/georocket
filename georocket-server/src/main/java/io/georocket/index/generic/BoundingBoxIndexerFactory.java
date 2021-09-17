package io.georocket.index.generic;

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

import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.georocket.query.ElasticsearchQueryHelper.geoShapeQuery;
import static io.georocket.query.ElasticsearchQueryHelper.shape;

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
