package de.fhg.igd.georocket.input;

/**
 * A simple class describing an XML start element with optional prefix,
 * namespaces and attributes.
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
    this.prefix = prefix;
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
}
