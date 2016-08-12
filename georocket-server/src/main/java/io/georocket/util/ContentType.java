package io.georocket.util;

import org.apache.commons.lang.StringUtils;
import rx.Observable;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A ContentType class to parse, compare content types and extract specific informations.
 * @author Andrej Sajenko
 */
public class ContentType {

  // type/specificType+subtype ; param ; param ; ...
  private String type;
  private String subtype;
  private String specificType;
  private Map<String, String> parameters;

  /**
   * <p>Example: application/gml+xml;version=1.0</p>
   * <p>Groups: 1/2/3/4</p>
   * <p>All Groups are always present but the values may be null!</p>
   */
  private static final Pattern pattern = Pattern.compile("(\\w*)\\/(\\w*\\+)?(\\w*)(.*)");

  private ContentType(String type, String subtype, String specificType, Map<String, String> parameters) {
    this.type = type;
    this.subtype = subtype;
    this.specificType = specificType;
    this.parameters = parameters;
  }

  /**
   * Parse a content type from a plain text string.
   *
   * <p>application/xml to type: application, subtype: xml</p>
   * <p>application/gml+xml to type: application, subtype: xml, specificType: gml</p>
   * <p>
   * application/xml;charset=UTF-8; version="1.0" to type: application, subtype: xml,
   * parameters: charset - UTF-8, version - 1.0
   * </p>
   *
   * @param text The content type to parse.
   *
   * @return The parsed content type.
   *
   * @throws IllegalArgumentException If the text was not a content type.
   */
  public static ContentType parse(String text) {
    Matcher matcher = pattern.matcher(text);
    if (matcher.matches()) {
      String type = matcher.group(1);

      String specific = "";
      if (matcher.groupCount() == 4) {
        specific = matcher.group(2);
        if (specific == null) {
          specific = "";
        } else {
          specific = StringUtils.strip(specific, "+");
        }
      }

      String subtype = matcher.group(3);
      String unparsedParams = matcher.group(4);
      Map<String, String> params = parseParams(unparsedParams);

      return new ContentType(type, subtype, specific, params);
    } else {
      throw new IllegalArgumentException("Invalid contentType input");
    }
  }

  private static Map<String, String> parseParams(String text) {
    Map<String, String> result = new HashMap<>();
    if (!text.trim().isEmpty()) {
      Observable.from(text.split(";"))
              .map(String::trim)
              .filter(e -> !e.isEmpty())
              .map(e -> e.split("="))
              .filter(e -> e.length > 0)
              .subscribe(e -> {
                String key = e[0];
                String value = "";
                if (e.length == 2) {
                  value = StringUtils.strip(e[1], "\"");
                }
                result.put(key, value);
              });
    }
    return result;
  }

  /**
   * @return The type <b>application</b> from the content type <i>application/xml</i>
   */
  public String getType() {
    return type;
  }

  /**
   * @return The subtype <b>xml</b> from the conten type <i>application/xml</i>
   */
  public String getSubtype() {
    return subtype;
  }

  /**
   * @return The specific type <b>gml</b> from the conten type <i>application/gml+xml</i>
   */
  public String getSpecificType() {
    return specificType;
  }

  /**
   * @return The parameters <b>charset: utf-8 and version: 1.0</b> from the
   * cointen type <i>application/xml; charset=utf-8;version="1.0"</i>
   */
  public Map<String, String> getParameters() {
    return new HashMap<>(parameters);
  }

  /**
   * Check: of any given content types contains this one.
   *
   * <p>
   *   See: ContentType{@link #belongsToContentType(ContentType)}
   * </p>
   *
   * @param contentTypes A collection of content types.
   *
   * @return True if this content type belongs to one of the contentTypes
   */
  public boolean belongsToContentType(String ... contentTypes) {
    for (String type: contentTypes) {
      if (this.belongsToContentType(type)) {
        return true;
      }
    }
    return false;
  }

  /**
   * <p>
   *   Check: does a content type belongs to another one.
   * </p>
   *
   * Examples:
   * <p>application/xml <b>belongsTo</b> application/xml</p>
   * <p>application/gml+xml <b>belongsTo</b> application/xml</p>
   * <p>text/xml <b>belongs not to</b> application/xml</p>
   *
   * @param contentType The content type which may contains this one.
   *
   * @return true if belongs to the given content type.
   */
  public boolean belongsToContentType(ContentType contentType) {
    if (!this.getType().equals(contentType.getType())) {
      return false;
    }
    if (!this.getSubtype().equals(contentType.getSubtype())) {
      return false;
    }
    if (!contentType.specificType.isEmpty()) {
      return this.specificType.equals(contentType.specificType);
    }
    return true;
  }

  /**
   * <p>
   *   Check: does a content type belongs to another one.
   * </p>
   *
   * Examples:
   * <p>application/xml <b>belongsTo</b> application/xml</p>
   * <p>application/gml+xml <b>belongsTo</b> application/xml</p>
   * <p>text/xml <b>belongs not to</b> application/xml</p>
   *
   * @param contentType The content type which may contains this one.
   *
   * @return true if belongs to the given content type.
   *
   * @throws IllegalArgumentException If the text was not a content type.
   */
  public boolean belongsToContentType(String contentType) {
    return this.belongsToContentType(ContentType.parse(contentType));
  }
}
