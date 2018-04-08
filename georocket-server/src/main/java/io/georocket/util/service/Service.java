package io.georocket.util.service;

import io.vertx.core.json.JsonObject;
import io.vertx.rxjava.core.Vertx;
import io.vertx.rxjava.servicediscovery.ServiceDiscovery;
import io.vertx.servicediscovery.Record;
import rx.Observable;
import rx.Single;

/**
 * Can be used to register a service by name and address, discover
 * all registered services and send or broadcast data to the registered
 * addresses.
 * @author Andrej Sajenko
 */
public interface Service {
  /**
   * Prefix for all counters created by this class
   */
  public static final String COUNTER_PREFIX = "io.georocket.service.counter-";

  /**
   * Discover all published services by name
   * @param name the service name
   * @param discovery the service discovery that should be used to discover the
   * services
   * @param vertx the Vert.x instance
   * @return all services published under the given the name
   * @see #publishOnce(Record, ServiceDiscovery, Vertx)
   */
  static Observable<Service> discover(String name, ServiceDiscovery discovery,
      Vertx vertx) {
    return discovery.rxGetRecords(r -> r.getName().equals(name))
        .flatMapObservable(Observable::from)
        .map(record -> {
          String endpoint = record.getLocation().getString(Record.ENDPOINT);
          Service s = new DefaultService(name, endpoint,
              record.getRegistration(), vertx);
          return s;
        });
  }

  /**
   * <p>Publish a service once. The combination of name and address should be
   * unique. If the name and address are already in use, this method will
   * complete successfully.</p>
   * <p><b>Note:</b> For every published service, a cluster-wide counter will
   * be created with the name
   * <code>{@link Service#COUNTER_PREFIX} + name + ":" + address</code>.</p>
   * @param name the service name
   * @param discovery the service discovery that should be used to publish the
   * service
   * @param vertx the Vert.x instance
   * @param address the service address on the event bus
   * @return a single emitting one item when the service has been registered
   */
  static Single<Void> publishOnce(String name, String address,
      ServiceDiscovery discovery, Vertx vertx) {
    Record record = new Record()
        .setName(name)
        .setLocation(new JsonObject().put(Record.ENDPOINT, address));

    if (name == null || name.trim().isEmpty()) {
      return Single.error(new IllegalArgumentException("Missing service name"));
    }
    if (address == null || address.trim().isEmpty()) {
      return Single.error(new IllegalArgumentException("Missing endpoint address"));
    }

    return publishOnce(record, discovery, vertx);
  }

  /**
   * <p>Publish a service (record) once. The combination of name
   * ({@link Record#getName()}) and endpoint (eventbus address,
   * {@link Record#getLocation()}.getString("endpoint")) has to be unique.
   * If the endpoint and name were already used, this method will complete
   * successfully.</p>
   * @param record the service to publish
   * @param discovery the service discovery that should be used to publish the
   * service
   * @param vertx the Vert.x instance
   * @return an observable emitting one item when the service has been registered
   */
  static Single<Void> publishOnce(Record record, ServiceDiscovery discovery,
      Vertx vertx) {
    String address = record.getLocation().getString(Record.ENDPOINT);
    String name = record.getName();

    if (name == null || name.trim().isEmpty()) {
      return Single.error(new IllegalArgumentException("Missing name in "
          + "service record"));
    }
    if (address == null || address.trim().isEmpty()) {
      return Single.error(new IllegalArgumentException("Missing endpoint "
          + "address in service record"));
    }

    String key = COUNTER_PREFIX + name + ":" + address;

    return vertx.sharedData().rxGetCounter(key)
        .flatMap(counter -> counter.rxCompareAndSet(0, 1))
        .flatMap(success -> {
          if (success) {
            // we're the first one to increment the counter
            // service can be registered
              return discovery.rxPublish(record).map(r -> null);
          }
          // service already published
          return Single.just(null);
        });
  }

  /**
   * <p>Send data to the registered endpoint of this service.</p>
   * <p>If there is more than one consumer for the endpoint address, <b>only
   * one</b> of them will receive the message.</p>
   * @param data the data to send to the service
   */
  void send(Object data);

  /**
   * <p>Send data to the registered endpoint of this service.</p>
   * <p>If there is more than one consumer for the endpoint address, <b>all</b> of
   * them will receive the message.</p>
   * @param data the data to broadcast to the service
   */
  void broadcast(Object data);

  /**
   * @return the endpoint address
   */
  String getAddress();

  /**
   * @return the service name
   */
  String getName();
  
  /**
   * Unpublish this service
   * @param discovery the service discovery where this service is registered
   * @return a single that emits one item when the operation has finished
   */
  Single<Void> unpublish(ServiceDiscovery discovery);
}
