package io.georocket.storage;

import io.georocket.output.Merger;

/**
 * The {@link StoreEventBusInterface} used to read XML.
 * 
 * @author Yasmina Kammeyer
 *
 */
public class XMLStoreEventBusInterface extends StoreEventBusInterface<Merger> {

  @Override
  protected Merger getMerger() {
    return new Merger();
  }

  @Override
  protected String getStoreEventBusInterfaceAddressExtension() {
    return ".XML";
  }

}
