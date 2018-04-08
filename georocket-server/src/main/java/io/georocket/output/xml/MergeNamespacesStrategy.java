package io.georocket.output.xml;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.commons.lang3.tuple.Pair;

import com.google.common.base.Splitter;

import io.georocket.storage.XMLChunkMeta;
import io.georocket.util.XMLStartElement;
import rx.Completable;
import rx.Single;

/**
 * Merge namespaces of XML root elements
 * @author Michel Kraemer
 */
public class MergeNamespacesStrategy extends AbstractMergeStrategy {
  /**
   * The namespaces of the current XML root elements
   */
  private List<Map<String, String>> currentNamespaces;
  
  /**
   * The attributes of the current XML root elements
   */
  private List<Map<Pair<String, String>, String>> currentAttributes;
  
  @Override
  public void setParents(List<XMLStartElement> parents) {
    currentNamespaces = new ArrayList<>();
    currentAttributes = new ArrayList<>();
    for (XMLStartElement e : parents) {
      // collect namespaces from parents
      Map<String, String> nss = new LinkedHashMap<>();
      currentNamespaces.add(nss);
      for (int i = 0; i < e.getNamespaceCount(); ++i) {
        String nsp1 = e.getNamespacePrefix(i);
        String nsu1 = e.getNamespaceUri(i);
        if (nsp1 == null) {
          nsp1 = "";
        }
        nss.put(nsp1, nsu1);
      }
      
      // collect attributes from parents
      Map<Pair<String, String>, String> as = new LinkedHashMap<>();
      currentAttributes.add(as);
      for (int i = 0; i < e.getAttributeCount(); ++i) {
        String ap = e.getAttributePrefix(i);
        String aln = e.getAttributeLocalName(i);
        String av = e.getAttributeValue(i);
        if (ap == null) {
          ap = "";
        }
        as.put(Pair.of(ap, aln), av);
      }
    }
    
    super.setParents(parents);
  }
  
  /**
   * Check if the namespaces of an XML start element can be merged in a map
   * of namespaces and if so update the map
   * @param namespaces the map of namespaces to update
   * @param e the element to merge
   * @param allowNew if the element may have namespaces that don't appear in
   * the given map
   * @return true if the element can be merged successfully
   */
  private static boolean canMergeNamespaces(Map<String, String> namespaces,
      XMLStartElement e, boolean allowNew) {
    for (int i = 0; i < e.getNamespaceCount(); ++i) {
      String nsp1 = e.getNamespacePrefix(i);
      String nsu1 = e.getNamespaceUri(i);
      if (nsp1 == null) {
        nsp1 = "";
      }
      String nsu2 = namespaces.get(nsp1);
      if (nsu2 == null) {
        if (allowNew) {
          namespaces.put(nsp1, nsu1);
        } else {
          return false;
        }
      } else if (!nsu1.equals(nsu2)) {
        // found same prefix, but different URI
        return false;
      }
    }
    return true;
  }
  
  /**
   * Check if the attributes of an XML start element can be merge in a map
   * of attributes and if so update the map
   * @param attributes the map of attributes to update
   * @param e the element to merge
   * @param allowNew if the element may have attributes that don't appear in
   * the given map
   * @return true if the element can be merged successfully
   */
  private static boolean canMergeAttributes(Map<String, String> attributes,
      XMLStartElement e, boolean allowNew) {
    for (int i = 0; i < e.getAttributeCount(); ++i) {
      String ap1 = e.getAttributePrefix(i);
      String aln1 = e.getAttributeLocalName(i);
      String av1 = e.getAttributeValue(i);
      if (ap1 == null) {
        ap1 = "";
      }
      String name1 = ap1 + ":" + aln1;
      
      String av2 = attributes.get(name1);
      if (av2 == null) {
        if (allowNew) {
          attributes.put(name1, av1);
        } else {
          return false;
        }
      } else {
        // ignore xsi:schemaLocation - we are able to merge this attribute
        if (name1.equals("xsi:schemaLocation")) {
          continue;
        }
        if (!av1.equals(av2)) {
          // found duplicate attribute, but different value
          return false;
        }
      }
    }
    return true;
  }
  
  /**
   * Check if two XML elements can be merged
   * @param e1 the first element
   * @param e2 the second element
   * @param allowNew true if e2 is allowed to have additional namespaces and
   * attributes that don't appear in e1
   * @return true if the elements can be merged
   */
  private static boolean canMerge(XMLStartElement e1, XMLStartElement e2,
      boolean allowNew) {
    // check name
    if (!e1.getName().equals(e2.getName())) {
      return false;
    }
    
    // check namespaces
    Map<String, String> namespaces = new HashMap<>();
    if (!canMergeNamespaces(namespaces, e1, true)) {
      return false;
    }
    if (!canMergeNamespaces(namespaces, e2, allowNew)) {
      return false;
    }
    
    // check attributes
    Map<String, String> attributes = new HashMap<>();
    if (!canMergeAttributes(attributes, e1, true)) {
      return false;
    }
    if (!canMergeAttributes(attributes, e2, allowNew)) {
      return false;
    }
    
    return true;
  }
  
  /**
   * Check if the given lists have the same size and all elements can be merged.
   * @param p1 the first list
   * @param p2 the second list
   * @param allowNew true if elements in p2 are allowed to have additional
   * namespaces and attributes that do not appear in the respective elements in p1
   * @return true if the two lists can be merged
   */
  private static boolean canMerge(List<XMLStartElement> p1, List<XMLStartElement> p2,
      boolean allowNew) {
    if (p1 == p2) {
      return true;
    }
    if (p1 == null || p2 == null) {
      return false;
    }
    if (p1.size() != p2.size()) {
      return false;
    }
    for (int i = 0; i < p1.size(); ++i) {
      if (!canMerge(p1.get(i), p2.get(i), allowNew)) {
        return false;
      }
    }
    return true;
  }
  
  @Override
  public Single<Boolean> canMerge(XMLChunkMeta meta) {
    if (getParents() == null || canMerge(getParents(), meta.getParents(),
        !isHeaderWritten())) {
      return Single.just(true);
    } else {
      return Single.just(false);
    }
  }
  
  @Override
  protected Completable mergeParents(XMLChunkMeta meta) {
    if (getParents() == null) {
      // no merge necessary yet, just save the chunk's parents
      setParents(meta.getParents());
      return Completable.complete();
    }
    
    // merge current parents and chunk parents
    List<XMLStartElement> newParents = new ArrayList<>();
    boolean changed = false;
    for (int i = 0; i < meta.getParents().size(); ++i) {
      XMLStartElement p = meta.getParents().get(i);
      Map<String, String> currentNamespaces = this.currentNamespaces.get(i);
      Map<Pair<String, String>, String> currentAttributes = this.currentAttributes.get(i);
      XMLStartElement newParent = mergeParent(p, currentNamespaces, currentAttributes);
      if (newParent == null) {
        newParent = this.getParents().get(i);
      } else {
        changed = true;
      }
      newParents.add(newParent);
    }
    if (changed) {
      super.setParents(newParents);
    }
    
    return Completable.complete();
  }
  
  /**
   * Merge an XML start element into a map of namespaces and a map of attributes
   * @param e the XML start element to merge
   * @param namespaces the current namespaces to merge into
   * @param attributes the current attributes to merge into
   * @return the merged element or {@code null} if no merge was necessary
   */
  private static XMLStartElement mergeParent(XMLStartElement e,
      Map<String, String> namespaces, Map<Pair<String, String>, String> attributes) {
    boolean changed = false;
    
    // merge namespaces
    for (int i = 0; i < e.getNamespaceCount(); ++i) {
      String nsp = e.getNamespacePrefix(i);
      if (nsp == null) {
        nsp = "";
      }
      if (!namespaces.containsKey(nsp)) {
        String nsu = e.getNamespaceUri(i);
        namespaces.put(nsp, nsu);
        changed = true;
      }
    }
    
    // merge attributes
    for (int i = 0; i < e.getAttributeCount(); ++i) {
      String ap = e.getAttributePrefix(i);
      String aln = e.getAttributeLocalName(i);
      if (ap == null) {
        ap = "";
      }
      Pair<String, String> name = Pair.of(ap, aln);
      if (!attributes.containsKey(name)) {
        // add new attribute
        String av = e.getAttributeValue(i);
        attributes.put(name, av);
        changed = true;
      } else if (ap.equals("xsi") && aln.equals("schemaLocation")) {
        // merge schema location
        String av = e.getAttributeValue(i);
        
        // find new schema locations and convert them to regular expressions
        List<String> avList = Splitter.on(' ')
            .trimResults()
            .omitEmptyStrings()
            .splitToList(av);
        List<Pair<String, String>> avRegExs = new ArrayList<>();
        for (int j = 0; j < avList.size(); j += 2) {
          String v = avList.get(j);
          String r = Pattern.quote(v);
          if (j + 1 < avList.size()) {
            String v2 = avList.get(j + 1);
            v += " " + v2;
            r += "\\s+" + Pattern.quote(v2);
          }
          avRegExs.add(Pair.of(r, v));
        }
        
        // test which new schema locations already exist in the
        // previous attribute value
        String existingAv = attributes.get(name);
        String newAv = "";
        for (Pair<String, String> avRegEx : avRegExs) {
          Pattern pattern = Pattern.compile(avRegEx.getKey());
          if (!pattern.matcher(existingAv).find()) {
            newAv += " " + avRegEx.getValue();
          }
        }
        
        // merge attribute values
        if (!newAv.isEmpty()) {
          attributes.put(name, existingAv + newAv);
          changed = true;
        }
      }
    }
    
    if (!changed) {
      // no need to create a new parent
      return null;
    }
    
    // create new merged parent
    return new XMLStartElement(e.getPrefix(), e.getLocalName(),
        namespaces.keySet().toArray(new String[namespaces.size()]),
        namespaces.values().toArray(new String[namespaces.size()]),
        attributes.keySet().stream().map(p -> p.getKey()).toArray(String[]::new),
        attributes.keySet().stream().map(p -> p.getValue()).toArray(String[]::new),
        attributes.values().toArray(new String[attributes.size()]));
  }
}
