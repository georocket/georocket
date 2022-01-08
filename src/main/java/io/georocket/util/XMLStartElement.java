package io.georocket.util;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.util.Arrays;

/**
 * A simple class describing an XML start element with optional prefix,
 * namespaces and attributes.
 * @since 1.0.0
 * @author Michel Kraemer
 */
public class XMLStartElement {
  private final String prefix;
  private final String localName;
  private final String[] namespacePrefixes;
  private final String[] namespaceUris;
  private final String[] attributePrefixes;
  private final String[] attributeLocalNames;
  private final String[] attributeValues;
  
  /**
   * Create a new start element with a given name
   * @param localName the name
   */
  public XMLStartElement(String localName) {
    this(null, localName);
  }
  
  /**
   * Create a new start element with a given prefix and name
   * @param prefix the prefix (may be null)
   * @param localName the name
   */
  public XMLStartElement(String prefix, String localName) {
    this(prefix, localName, null, null);
  }
  
  /**
   * Create a new start element with a given prefix, name and namespaces
   * @param prefix the prefix (may be null)
   * @param localName the name
   * @param namespacePrefixes the namespace prefixes (may be null)
   * @param namespaceUris the namespace URIs (may be null)
   */
  public XMLStartElement(String prefix, String localName, String[] namespacePrefixes,
      String[] namespaceUris) {
    this(prefix, localName, namespacePrefixes, namespaceUris, null, null, null);
  }
  
  /**
   * Create a new start element with a given prefix, name and attributes
   * @param prefix the prefix (may be null)
   * @param localName the name
   * @param attributePrefixes the attribute prefixes (may be null)
   * @param attributeLocalNames the attribute names (may be null)
   * @param attributeValues the attribute values (may be null)
   */
  public XMLStartElement(String prefix, String localName, String[] attributePrefixes,
      String[] attributeLocalNames,  String[] attributeValues) {
    this(prefix, localName, null, null, attributePrefixes, attributeLocalNames, attributeValues);
  }
  
  /**
   * Create a new start element with a given prefix, name and namespaces
   * @param prefix the prefix (may be null)
   * @param localName the name
   * @param namespacePrefixes the namespace prefixes (may be null)
   * @param namespaceUris the namespace URIs (may be null)
   * @param attributePrefixes the attribute prefixes (may be null)
   * @param attributeLocalNames the attribute names (may be null)
   * @param attributeValues the attribute values (may be null)
   */
  public XMLStartElement(String prefix, String localName, String[] namespacePrefixes,
      String[] namespaceUris, String[] attributePrefixes, String[] attributeLocalNames,
      String[] attributeValues) {
    if ((namespacePrefixes == null || namespaceUris == null) && namespacePrefixes != namespaceUris) {
      throw new IllegalArgumentException("namespacePrefixes and namespaceUris must either be both null or non-null");
    }
    if ((attributePrefixes == null || attributeLocalNames == null || attributeValues == null) &&
        (attributePrefixes != attributeLocalNames || attributePrefixes != attributeValues)) {
      throw new IllegalArgumentException("attributePrefixes, attributeLocalNames and attributeValues "
          + "must either be all null or non-null");
    }
    if (namespacePrefixes != null && namespacePrefixes.length != namespaceUris.length) {
      throw new IllegalArgumentException("namespacePrefixes and namespaceUris must have the same number of elements");
    }
    if (attributePrefixes != null && (attributePrefixes.length != attributeLocalNames.length ||
        attributePrefixes.length != attributeValues.length)) {
      throw new IllegalArgumentException("attributePrefixes, attributeLocalNames and attributeValues "
          + "must have the same number of elements");
    }
    this.prefix = prefix == null || prefix.isEmpty() ? null : prefix;
    this.localName = localName;
    this.namespacePrefixes = namespacePrefixes;
    this.namespaceUris = namespaceUris;
    this.attributePrefixes = attributePrefixes;
    this.attributeLocalNames = attributeLocalNames;
    this.attributeValues = attributeValues;
  }
  
  /**
   * @return the element's prefix or null if it doesn't have one
   */
  public String getPrefix() {
    return prefix;
  }
  
  /**
   * @return the element's local name
   */
  public String getLocalName() {
    return localName;
  }
  
  /**
   * @return the elements name consisting of the prefix and the local name
   */
  public String getName() {
    if (prefix != null && !prefix.isEmpty()) {
      return prefix + ":" + localName;
    }
    return localName;
  }
  
  /**
   * @return the number of namespaces attached to this element
   */
  public int getNamespaceCount() {
    return namespacePrefixes != null ? namespacePrefixes.length : 0;
  }
  
  /**
   * Get a namespace prefix at a given position
   * @param i the position
   * @return the prefix
   */
  public String getNamespacePrefix(int i) {
    return namespacePrefixes[i];
  }
  
  /**
   * Get a namespace URI at a given position
   * @param i the position
   * @return the URI
   */
  public String getNamespaceUri(int i) {
    return namespaceUris[i];
  }
  
  /**
   * @return the number of attributes attached to this element
   */
  public int getAttributeCount() {
    return attributePrefixes != null ? attributePrefixes.length : 0;
  }
  
  /**
   * Get an attribute prefix at a given position
   * @param i the position
   * @return the prefix
   */
  public String getAttributePrefix(int i) {
    return attributePrefixes[i];
  }
  
  /**
   * Get the local name of an attribute at a given position
   * @param i the position
   * @return the local name
   */
  public String getAttributeLocalName(int i) {
    return attributeLocalNames[i];
  }
  
  /**
   * Get an attribute value at a given position
   * @param i the position
   * @return the value
   */
  public String getAttributeValue(int i) {
    return attributeValues[i];
  }
  
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("<" + getName() + "");
    
    for (int i = 0; i < getNamespaceCount(); ++i) {
      sb.append(" xmlns");
      String prefix = getNamespacePrefix(i);
      if (prefix != null && !prefix.isEmpty()) {
        sb.append(":");
        sb.append(prefix);
      }
      sb.append("=\"");
      sb.append(getNamespaceUri(i));
      sb.append("\"");
    }
    
    for (int i = 0; i < getAttributeCount(); ++i) {
      sb.append(" ");
      String prefix = getAttributePrefix(i);
      if (prefix != null && !prefix.isEmpty()) {
        sb.append(prefix);
        sb.append(":");
      }
      sb.append(getAttributeLocalName(i));
      sb.append("=\"");
      sb.append(getAttributeValue(i));
      sb.append("\"");
    }
    
    sb.append(">");
    
    return sb.toString();
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + Arrays.hashCode(attributeLocalNames);
    result = prime * result + Arrays.hashCode(attributePrefixes);
    result = prime * result + Arrays.hashCode(attributeValues);
    result = prime * result + ((localName == null) ? 0 : localName.hashCode());
    result = prime * result + Arrays.hashCode(namespacePrefixes);
    result = prime * result + Arrays.hashCode(namespaceUris);
    result = prime * result + ((prefix == null) ? 0 : prefix.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    XMLStartElement other = (XMLStartElement)obj;
    if (!Arrays.equals(attributeLocalNames, other.attributeLocalNames)) {
      return false;
    }
    if (!Arrays.equals(attributePrefixes, other.attributePrefixes)) {
      return false;
    }
    if (!Arrays.equals(attributeValues, other.attributeValues)) {
      return false;
    }
    if (localName == null) {
      if (other.localName != null) {
        return false;
      }
    } else if (!localName.equals(other.localName)) {
      return false;
    }
    if (!Arrays.equals(namespacePrefixes, other.namespacePrefixes)) {
      return false;
    }
    if (!Arrays.equals(namespaceUris, other.namespaceUris)) {
      return false;
    }
    if (prefix == null) {
      if (other.prefix != null) {
        return false;
      }
    } else if (!prefix.equals(other.prefix)) {
      return false;
    }
    return true;
  }
  
  /**
   * @return this object as a {@link JsonObject}
   */
  public JsonObject toJsonObject() {
    JsonArray np = new JsonArray();
    if (namespacePrefixes != null) {
      Arrays.asList(namespacePrefixes).forEach(e -> np.add(e));
    }
    JsonArray nu = new JsonArray();
    if (namespaceUris != null) {
      Arrays.asList(namespaceUris).forEach(e -> nu.add(e));
    }
    JsonArray ap = new JsonArray();
    if (attributePrefixes != null) {
      Arrays.asList(attributePrefixes).forEach(e -> ap.add(e));
    }
    JsonArray aln = new JsonArray();
    if (attributeLocalNames != null) {
      Arrays.asList(attributeLocalNames).forEach(e -> aln.add(e));
    }
    JsonArray av = new JsonArray();
    if (attributeValues != null) {
      Arrays.asList(attributeValues).forEach(e -> av.add(e));
    }
    return new JsonObject()
        .put("prefix", prefix)
        .put("localName", localName)
        .put("namespacePrefixes", np)
        .put("namespaceUris", nu)
        .put("attributePrefixes", ap)
        .put("attributeLocalNames", aln)
        .put("attributeValues", av);
  }
  
  /**
   * Converts a {@link JsonObject} to a {@link XMLStartElement}
   * @param obj the {@link JsonObject} to convert
   * @return the {@link XMLStartElement}
   */
  public static XMLStartElement fromJsonObject(JsonObject obj) {
    JsonArray np = obj.getJsonArray("namespacePrefixes");
    JsonArray nu = obj.getJsonArray("namespaceUris");
    JsonArray ap = obj.getJsonArray("attributePrefixes");
    JsonArray aln = obj.getJsonArray("attributeLocalNames");
    JsonArray av = obj.getJsonArray("attributeValues");
    return new XMLStartElement(obj.getString("prefix"), obj.getString("localName"),
        np.stream().toArray(String[]::new), nu.stream().toArray(String[]::new),
        ap.stream().toArray(String[]::new), aln.stream().toArray(String[]::new),
        av.stream().toArray(String[]::new));
  }
}
