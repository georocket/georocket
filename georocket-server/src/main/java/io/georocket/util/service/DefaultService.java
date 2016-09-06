package io.georocket.util.service;

import io.vertx.rxjava.core.Vertx;

/**
 * Default implementation of {@link Service}
 * @author Michel Kraemer
 */
public class DefaultService implements Service {
  private final String name;
  private final String address;
  private final Vertx vertx;

  /**
   * Create a new service
   * @param name the service name
   * @param address the eventbus address
   * @param vertx the Vert.x instance
   */
  public DefaultService(String name, String address, Vertx vertx) {
    this.name = name;
    this.address = address;
    this.vertx = vertx;
  }

  @Override
  public void send(Object data) {
    vertx.eventBus().send(address, data);
  }

  @Override
  public void broadcast(Object data) {
    vertx.eventBus().publish(address, data);
  }

  @Override
  public String getAddress() {
    return address;
  }

  @Override
  public String getName() {
    return name;
  }
}
