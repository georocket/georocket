package io.georocket.service;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.core.shareddata.Counter;
import io.vertx.rx.java.ObservableFuture;
import io.vertx.rx.java.RxHelper;
import io.vertx.servicediscovery.Record;
import io.vertx.servicediscovery.ServiceDiscovery;
import rx.Observable;

import java.util.List;

/**
 * Can be used to register a Service by Name and address, discover
 * all registered Services and send or broadcast data to the registered
 * addresses.
 *
 * @author Andrej Sajenko
 */
public interface Service {

  public static final String COUNTER_PREFIX = "io.georocket.service.counter-";

  /**
   * Discover all published services by name.
   *
   * See {@link #publishOnce(Vertx, Record)}
   *
   * @param vertx the vertx instance
   * @param name The name of the service.
   *
   * @return All Services published by the name
   */
  static Observable<Service> discover(Vertx vertx, String name) {
    ServiceDiscovery serviceDiscovery = ServiceDiscovery.create(vertx);

    ObservableFuture<List<Record>> serviceObservable = RxHelper.observableFuture();
    serviceDiscovery.getRecords(r -> r.getName().equals(name), serviceObservable.toHandler());

    return serviceObservable.flatMap(Observable::from).map(record -> (Service)new Service() {
      private String endpoint = record.getLocation().getString(Record.ENDPOINT);

      @Override
      public void send(Object data) {
        if (endpoint != null) {
          vertx.eventBus().send(endpoint, data);
        }
      }

      @Override
      public void broadcast(Object data) {
        vertx.eventBus().publish(endpoint, data);
      }

      @Override
      public String getAddress() {
        return endpoint;
      }

      @Override
      public String getName() {
        return name;
      }

    }).doOnTerminate(serviceDiscovery::close);
  }

  /**
   * <p>Publish a service once.
   * The combination of name and address has to be unique.</p>
   *
   * <p>If the name and address were already used, publish will complete
   * successfully.</p>
   *
   * <p><b>Note:</b> For every published service, a cluster-wide counter will be created
   * with name ({@link Service#COUNTER_PREFIX} + name + ":" + address)
   * </p>
   *
   * @param vertx The vertx instance
   * @param name The name of a service
   * @param address The address of the service
   *
   * @return The observer which will be filled when completed or failed.
   */
  static Observable<Void> publishOnce(Vertx vertx, String name, String address) {
    Record record = new Record()
            .setName(name)
            .setLocation(new JsonObject().put(Record.ENDPOINT, address));

    if (name == null || name.trim().isEmpty()) {
      return Observable.error(new IllegalArgumentException("Missing the name of this " +
              "service"));
    }
    if (address == null || address.trim().isEmpty()) {
      return Observable.error(new IllegalArgumentException("Missing the endpoint " +
              "address"));
    }

    return publishOnce(vertx, record);
  }

  /**
   * Publish a service (=record) once.
   * The combination of name and address (endpoint) has to be unique.
   *
   * Record#getLocation().getString("endpoint") and Record#getName() has to be unique.
   *
   * If the endpoint and name were already used, publish will complete
   * successfully.
   *
   * @param vertx The vertx instance
   * @param record The service to publish
   *
   * @return The observer which will be filled when completed or failed.
   */
  static Observable<Void> publishOnce(Vertx vertx, Record record) {

    String address = record.getLocation().getString(Record.ENDPOINT);
    String name = record.getName();

    if (name == null || name.trim().isEmpty()) {
      return Observable.error(new IllegalArgumentException("Missing the name of this " +
              "service. You have to set the name with Record#setName()"));
    }
    if (address == null || address.trim().isEmpty()) {
      return Observable.error(new IllegalArgumentException("Missing the endpoint " +
              "address. You have to set the name with Record#getLocation().put(Record" +
              ".ENDPOINT, \"<address>\")."));
    }

    String key = COUNTER_PREFIX + name + ":" + address;

    ServiceDiscovery serviceDiscovery = ServiceDiscovery.create(vertx);

    ObservableFuture<Record> observableFuture = RxHelper.observableFuture();

    Handler<AsyncResult<Record>> handler = observableFuture.toHandler();

    vertx.sharedData().getCounter(key, ar -> {
      if (ar.failed()) {
        handler.handle(Future.failedFuture(ar.cause()));
      } else {
        Counter counter = ar.result();
        counter.compareAndSet(0, 1, ha -> {
          if (ha.failed()) {
            handler.handle(Future.failedFuture(ha.cause()));
          } else {
            Boolean success = ha.result();
            if (success) {
              // Hey, i was the first one who incremented the counter, i can register
              // the record
              serviceDiscovery.publish(record, handler);
            } else {
              // Service already published
              handler.handle(Future.succeededFuture(record));
            }
          }
        });
      }
    });

    return observableFuture.<Void>map(e -> null).doOnTerminate(serviceDiscovery::close);
  }

  /**
   * <p>Send the data to the registered endpoint of this service.</p>
   *
   * <p>If there is more than one consumer for the endpoint address, <b>only
   * one</b> of them will receive the message.</p>
   *
   * @param data The data to send to the service
   */
  void send(Object data);

  /**
   * <p>Send the data to the registered endpoint of this service.</p>
   *
   * <p>If there is more than one consumer for the endpoint address, <b>all</b> of
   * them will receive the message.</p>
   *
   * @param data The data to broadcast to the service.
   */
  void broadcast(Object data);

  /**
   * @return the endpoint address.
   */
  String getAddress();


  /**
   * @return the service name.
   */
  String getName();

}
