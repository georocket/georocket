package io.georocket.output.xml

import io.georocket.storage.XmlChunkMeta
import io.georocket.util.XMLStartElement
import java.util.regex.Pattern

/**
 * Merge namespaces of XML root elements
 * @author Michel Kraemer
 */
class MergeNamespacesStrategy : AbstractMergeStrategy() {
  /**
   * The namespaces of the current XML root elements
   */
  private var currentNamespaces: MutableList<MutableMap<String?, String>>? = null

  /**
   * The attributes of the current XML root elements
   */
  private var currentAttributes: MutableList<MutableMap<Pair<String?, String>, String>>? = null

  override var parents: List<XMLStartElement>?
    get() = super.parents
    set(parents) {
      currentNamespaces = mutableListOf()
      currentAttributes = mutableListOf()
      for (e in parents!!) {
        // collect namespaces from parents
        val nss = mutableMapOf<String?, String>()
        currentNamespaces!!.add(nss)
        for (i in 0 until e.namespaceCount) {
          val nsp1 = e.getNamespacePrefix(i)
          val nsu1 = e.getNamespaceUri(i)
          nss[nsp1] = nsu1
        }

        // collect attributes from parents
        val attrs = mutableMapOf<Pair<String?, String>, String>()
        currentAttributes!!.add(attrs)
        for (i in 0 until e.attributeCount) {
          val ap = e.getAttributePrefix(i)
          val aln = e.getAttributeLocalName(i)
          val av = e.getAttributeValue(i)
          attrs[ap to aln] = av
        }
      }
      super.parents = parents
    }

  override fun canMerge(metadata: XmlChunkMeta): Boolean {
    return (parents == null || canMerge(parents, metadata.parents, !isHeaderWritten))
  }

  override fun mergeParents(chunkMetadata: XmlChunkMeta) {
    if (parents == null) {
      // no merge necessary yet, just save the chunk's parents
      parents = chunkMetadata.parents
      return
    }

    // merge current parents and chunk parents
    val newParents = mutableListOf<XMLStartElement>()
    var changed = false
    for (i in chunkMetadata.parents.indices) {
      val p = chunkMetadata.parents[i]
      val currentNamespaces = currentNamespaces!![i]
      val currentAttributes = currentAttributes!![i]
      var newParent = mergeParent(p, currentNamespaces, currentAttributes)
      if (newParent == null) {
        newParent = parents!![i]
      } else {
        changed = true
      }
      newParents.add(newParent)
    }
    if (changed) {
      super.parents = newParents
    }
  }

  /**
   * Check if the [namespaces] of an XML start element [e] can be merged in a
   * map of namespaces and if so update the map. Callers may set [allowNew] to
   * `true` if the element has namespaces that don't appear in the given
   * map. Return `true` if the element can be merged successfully.
   */
  private fun canMergeNamespaces(namespaces: MutableMap<String, String>,
      e: XMLStartElement, allowNew: Boolean): Boolean {
    for (i in 0 until e.namespaceCount) {
      var nsp1 = e.getNamespacePrefix(i)
      val nsu1 = e.getNamespaceUri(i)
      if (nsp1 == null) {
        nsp1 = ""
      }
      val nsu2 = namespaces[nsp1]
      if (nsu2 == null) {
        if (allowNew) {
          namespaces[nsp1] = nsu1
        } else {
          return false
        }
      } else if (nsu1 != nsu2) {
        // found same prefix, but different URI
        return false
      }
    }
    return true
  }

  /**
   * Check if the [attributes] of an XML start element [e] can be merge in a
   * map of attributes and if so update the map. Callers may set [allowNew] to
   * `true` if the element has attributes that don't appear in the given
   * map. Return `true` if the element can be merged successfully.
   */
  private fun canMergeAttributes(attributes: MutableMap<String, String>,
      e: XMLStartElement, allowNew: Boolean): Boolean {
    for (i in 0 until e.attributeCount) {
      var ap1 = e.getAttributePrefix(i)
      val aln1 = e.getAttributeLocalName(i)
      val av1 = e.getAttributeValue(i)
      if (ap1 == null) {
        ap1 = ""
      }
      val name1 = "$ap1:$aln1"
      val av2 = attributes[name1]
      if (av2 == null) {
        if (allowNew) {
          attributes[name1] = av1
        } else {
          return false
        }
      } else {
        // ignore xsi:schemaLocation - we are able to merge this attribute
        if (name1 == "xsi:schemaLocation") {
          continue
        }
        if (av1 != av2) {
          // found duplicate attribute, but different value
          return false
        }
      }
    }
    return true
  }

  /**
   * Check if two XML elements [e1] and [e2] can be merged. Callers may set
   * [allowNew] to `true` if [e2] is allowed to have additional namespaces and
   * attributes that don't appear in [e1]. Return `true` if the elements can
   * be merged
   */
  private fun canMerge(e1: XMLStartElement, e2: XMLStartElement,
      allowNew: Boolean): Boolean {
    // check name
    if (e1.name != e2.name) {
      return false
    }

    // check namespaces
    val namespaces = mutableMapOf<String, String>()
    if (!canMergeNamespaces(namespaces, e1, true)) {
      return false
    }
    if (!canMergeNamespaces(namespaces, e2, allowNew)) {
      return false
    }

    // check attributes
    val attributes = mutableMapOf<String, String>()
    if (!canMergeAttributes(attributes, e1, true)) {
      return false
    }

    return canMergeAttributes(attributes, e2, allowNew)
  }

  /**
   * Check if the lists [p1] and [p2] have the same size and all elements can
   * be merged. [allowNew] should be set to `true` if elements in [p2] are
   * allowed to have additional namespaces and attributes that do not appear
   * in the respective elements in [p1]. Return `true` if the two lists can
   * be merged
   */
  private fun canMerge(p1: List<XMLStartElement>?, p2: List<XMLStartElement>?,
      allowNew: Boolean): Boolean {
    if (p1 === p2) {
      return true
    }
    if (p1 == null || p2 == null) {
      return false
    }
    if (p1.size != p2.size) {
      return false
    }
    for (i in p1.indices) {
      if (!canMerge(p1[i], p2[i], allowNew)) {
        return false
      }
    }
    return true
  }

  /**
   * Merge an XML start element [e] into a map of [namespaces] and a map of
   * [attributes]. Return the merged element or `null` if no merge was necessary
   */
  private fun mergeParent(e: XMLStartElement, namespaces: MutableMap<String?, String>,
      attributes: MutableMap<Pair<String?, String>, String>): XMLStartElement? {
    var changed = false

    // merge namespaces
    for (i in 0 until e.namespaceCount) {
      val nsp = e.getNamespacePrefix(i)
      if (!namespaces.containsKey(nsp)) {
        val nsu = e.getNamespaceUri(i)
        namespaces[nsp] = nsu
        changed = true
      }
    }

    // merge attributes
    for (i in 0 until e.attributeCount) {
      val ap = e.getAttributePrefix(i)
      val aln = e.getAttributeLocalName(i)
      val name = ap to aln
      if (!attributes.containsKey(name)) {
        // add new attribute
        val av = e.getAttributeValue(i)
        attributes[name] = av
        changed = true
      } else if (ap == "xsi" && aln == "schemaLocation") {
        // merge schema location
        val av = e.getAttributeValue(i)

        // find new schema locations and convert them to regular expressions
        val avList = av.split(' ').map { it.trim() }.filter { it.isNotEmpty() }
        val avRegExs = mutableListOf<Pair<String, String>>()
        var j = 0
        while (j < avList.size) {
          var v = avList[j]
          var r = Pattern.quote(v)
          if (j + 1 < avList.size) {
            val v2 = avList[j + 1]
            v += " $v2"
            r += "\\s+" + Pattern.quote(v2)
          }
          avRegExs.add(r to v)
          j += 2
        }

        // test which new schema locations already exist in the
        // previous attribute value
        val existingAv = attributes[name]!!
        var newAv = ""
        for ((key, value) in avRegExs) {
          val pattern = Pattern.compile(key)
          if (!pattern.matcher(existingAv).find()) {
            newAv += " $value"
          }
        }

        // merge attribute values
        if (newAv.isNotEmpty()) {
          attributes[name] = existingAv + newAv
          changed = true
        }
      }
    }

    return if (!changed) {
      // no need to create a new parent
      null
    } else {
      // create new merged parent
      val orderedNamespaces = namespaces.entries.toList()
      val orderedAttributes = attributes.entries.toList()
      XMLStartElement(e.prefix, e.localName,
        namespacePrefixes = orderedNamespaces.map { it.key },
        namespaceUris = orderedNamespaces.map { it.value },
        attributePrefixes = orderedAttributes.map { it.key.first },
        attributeLocalNames = orderedAttributes.map { it.key.second },
        attributeValues = orderedAttributes.map { it.value },
      )
    }
  }
}
