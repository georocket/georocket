package io.georocket.storage;

import io.georocket.output.Merger;

/**
 * The {@link StoreMessenger} used to read XML.
 * 
 * @author Yasmina Kammeyer
 *
 */
public class XMLStoreMessenger extends StoreMessenger<Merger> {

  @Override
  protected Merger getMerger() {
    return new Merger();
  }

  @Override
  protected String getStoreServiceAddressExtension() {
    return ".XML";
  }

}
