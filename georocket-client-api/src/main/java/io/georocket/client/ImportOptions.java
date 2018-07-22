package io.georocket.client;

import io.vertx.core.Handler;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

/**
 * Parameters that can be passed to {@link StoreClient#startImport(ImportOptions, Handler)}
 * @since 1.3.0
 * @author Michel Kraemer
 */
public class ImportOptions {
  /**
   * A compression method
   */
  public enum Compression {
    /**
     * Data is sent uncompressed
     */
    NONE,

    /**
     * Data is sent using GZIP compression
     */
    GZIP
  }

  private String layer;
  private Collection<String> tags;
  private Collection<String> properties;
  private Long size;
  private String fallbackCRS;
  private Compression compression = Compression.NONE;

  /**
   * Set the layer to import to
   * @param layer the layer (may be <code>null</code> if data
   * should be imported to the root layer)
   * @return a reference to this, so the API can be used fluently
   */
  public ImportOptions setLayer(String layer) {
    this.layer = layer;
    return this;
  }

  /**
   * Get the layer to import to
   * @return the layer to import to (may be <code>null</code> if data
   * should be imported to the root layer)
   */
  public String getLayer() {
    return layer;
  }

  /**
   * Set a collection of tags to attach to the imported data
   * @param tags the tags (may be <code>null</code> if no tags should
   * be attached)
   * @return a reference to this, so the API can be used fluently
   */
  public ImportOptions setTags(Collection<String> tags) {
    this.tags = tags;
    return this;
  }

  /**
   * Add items to the collection of tags to attach to the imported data
   * @param tags the tags to add
   * @return a reference to this, so the API can be used fluently
   */
  public ImportOptions addTags(Collection<String> tags) {
    if (tags == null || tags.isEmpty()) {
      return this;
    }
    if (this.tags == null) {
      this.tags = new ArrayList<>();
    }
    this.tags.addAll(tags);
    return this;
  }

  /**
   * Add items to the collection of tags to attach to the imported data
   * @param tags the tags to add
   * @return a reference to this, so the API can be used fluently
   */
  public ImportOptions addTags(String... tags) {
    if (tags == null || tags.length == 0) {
      return this;
    }
    if (this.tags == null) {
      this.tags = new ArrayList<>();
    }
    this.tags.addAll(Arrays.asList(tags));
    return this;
  }

  /**
   * Add a tag to the collection of tags to attach to the imported data
   * @param tag the tag to add
   * @return a reference to this, so the API can be used fluently
   */
  public ImportOptions addTag(String tag) {
    addTags(tag);
    return this;
  }

  /**
   * Get the collection of tags to attach to the imported data
   * @return the tags to attach to the imported data (<code>null</code>
   * if no tags will be attached)
   */
  public Collection<String> getTags() {
    return tags;
  }

  /**
   * Set a collection of properties to attach to the imported data
   * @param properties the properties (may be <code>null</code> if no
   * properties should be attached)
   * @return a reference to this, so the API can be used fluently
   */
  public ImportOptions setProperties(Collection<String> properties) {
    this.properties = properties;
    return this;
  }

  /**
   * Add items to the collection of properties to attach to the
   * imported data
   * @param properties the properties to add
   * @return a reference to this, so the API can be used fluently
   */
  public ImportOptions addProperties(Collection<String> properties) {
    if (properties == null || properties.isEmpty()) {
      return this;
    }
    if (this.properties == null) {
      this.properties = new ArrayList<>();
    }
    this.properties.addAll(properties);
    return this;
  }

  /**
   * Add items to the collection of properties to attach to the
   * imported data
   * @param properties the properties to add
   * @return a reference to this, so the API can be used fluently
   */
  public ImportOptions addProperties(String... properties) {
    if (properties == null || properties.length == 0) {
      return this;
    }
    if (this.properties == null) {
      this.properties = new ArrayList<>();
    }
    this.properties.addAll(Arrays.asList(properties));
    return this;
  }

  /**
   * Add a property to the collection of properties to attach to the
   * imported data
   * @param property the property to add
   * @return a reference to this, so the API can be used fluently
   */
  public ImportOptions addProperty(String property) {
    addProperties(property);
    return this;
  }

  /**
   * Get the collection of properties to attach to the imported data
   * @return the properties to attach to the imported data (<code>null</code>
   * if no properties will be attached)
   */
  public Collection<String> getProperties() {
    return properties;
  }

  /**
   * Set the size of the data to be sent in bytes. If compression is
   * enabled (see {@link #setCompression(Compression)}) this is the number
   * of compressed bytes and not the size of the raw uncompressed data.
   * @param size the size of data to be sent in bytes (may be {@code null}
   * if unknown)
   * @return a reference to this, so the API can be used fluently
   */
  public ImportOptions setSize(Long size) {
    this.size = size;
    return this;
  }

  /**
   * Get the size of data to be sent in bytes. If compression is
   * enabled (see {@link #setCompression(Compression)}) this is the number
   * of compressed bytes and not the size of the raw uncompressed data.
   * @return the size (may be {@code null} if unknown)
   */
  public Long getSize() {
    return size;
  }

  /**
   * Set the CRS that should be used if the imported file does
   * not specify one
   * @param fallbackCRS the CRS (may be {@code null})
   * @return a reference to this, so the API can be used fluently
   */
  public ImportOptions setFallbackCRS(String fallbackCRS) {
    this.fallbackCRS = fallbackCRS;
    return this;
  }

  /**
   * Get the CRS that should be used if the imported file does
   * not specify one
   * @return the CRS
   */
  public String getFallbackCRS() {
    return fallbackCRS;
  }

  /**
   * Set the compression method that is applied to the data that will be
   * sent to the {@link io.vertx.core.streams.WriteStream} returned by
   * {@link StoreClient#startImport(ImportOptions, Handler)}. The caller
   * is responsible for compressing the data, for example by wrapping
   * the {@link io.vertx.core.streams.WriteStream} into a
   * {@link io.georocket.util.io.GzipWriteStream}.
   * @param compression the compression method (may be {@code null} if
   * no compression should be used)
   * @return a reference to this, so the API can be used fluently
   */
  public ImportOptions setCompression(Compression compression) {
    if (compression == null) {
      this.compression = Compression.NONE;
    } else {
      this.compression = compression;
    }
    return this;
  }

  /**
   * Get the compression method
   * @return the compression method (never {@code null})
   */
  public Compression getCompression() {
    return compression;
  }
}
