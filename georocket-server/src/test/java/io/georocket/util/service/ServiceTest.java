package io.georocket.util.service;

import java.util.List;
import java.util.stream.Collectors;

import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.RunTestOnContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.rxjava.core.Vertx;
import io.vertx.rxjava.servicediscovery.ServiceDiscovery;
import io.vertx.servicediscovery.Record;

/**
 * Test the implementation of {@link Service}
 * @author Andrej Sajenko
 */
@RunWith(VertxUnitRunner.class)
public class ServiceTest {
  /**
   * Run the test on a Vert.x test context
   */
  @Rule
  public RunTestOnContext rule = new RunTestOnContext();

  /**
   * Test if a service is really published only once
   * @param context the test context
   */
  @Test
  public void publishOnce(TestContext context) {
    Vertx vertx = new Vertx(rule.vertx());
    Async async = context.async();

    ServiceDiscovery discovery = ServiceDiscovery.create(vertx);
    Service.publishOnce("A", "A", discovery, vertx)
      .flatMap(v -> Service.publishOnce("A", "A", discovery, vertx))
      .flatMap(v -> Service.publishOnce("A", "B", discovery, vertx))
      .flatMap(v -> {
        return discovery.getRecordsObservable(record -> true);
      })
      .doOnTerminate(discovery::close)
      .subscribe(recordList -> {
        List<Record> lA = recordList.stream()
            .filter(r -> r.getName().equals("A"))
            .collect(Collectors.toList());
        context.assertEquals(2, lA.size());
        async.complete();
      }, context::fail);
  }

  /**
   * Test if a service can be discovered
   * @param context the test context
   */
  @Test
  public void discover(TestContext context) {
    Vertx vertx = new Vertx(rule.vertx());
    Async async = context.async();

    ServiceDiscovery discovery = ServiceDiscovery.create(vertx);
    Service.publishOnce("A", "a", discovery, vertx)
      .flatMap(v -> Service.publishOnce("A", "b", discovery, vertx))
      .flatMap(v -> Service.discover("A", discovery, vertx))
      .count()
      .doOnTerminate(discovery::close)
      .subscribe(count -> {
        context.assertEquals(2, count);
        async.complete();
      }, context::fail);
  }

  /**
   * Test if a message can be sent and if a consumer receives the message.
   * This method does not test if the message can be sent to only one
   * consumer (instead of all, see {@link #broadcast(TestContext)}),
   * because it's not possible to check if a consumer will not receive a
   * message before the asynchronous test ends.
   * @param context the test context
   */
  @Test
  public void send(TestContext context) {
    Vertx vertx = new Vertx(rule.vertx());

    Async a = context.async();

    String data = "special data";

    vertx.eventBus().consumer("a", h -> {
      context.assertEquals(data, h.body());
      a.complete();
    });

    ServiceDiscovery discovery = ServiceDiscovery.create(vertx);
    Service.publishOnce("A", "a", discovery, vertx)
      .flatMap(v -> Service.discover("A", discovery, vertx))
      .doOnTerminate(discovery::close)
      .subscribe(service -> {
        service.broadcast(data);
        service.send(data);
      }, context::fail);
  }

  /**
   * Test if a message can be sent to all consumers
   * @param context the test context
   */
  @Test
  public void broadcast(TestContext context) {
    Vertx vertx = new Vertx(rule.vertx());

    Async aC = context.async();
    Async bC = context.async();
    Async cC = context.async();
    Async dC = context.async();

    String data = "special data";

    vertx.eventBus().consumer("a", h -> {
      context.assertEquals(data, h.body());
      aC.complete();
    });
    vertx.eventBus().consumer("a", h -> {
      context.assertEquals(data, h.body());
      bC.complete();
    });
    vertx.eventBus().consumer("b", h -> {
      context.assertEquals(data, h.body());
      cC.complete();
    });
    vertx.eventBus().consumer("b", h -> {
      context.assertEquals(data, h.body());
      dC.complete();
    });

    ServiceDiscovery discovery = ServiceDiscovery.create(vertx);
    Service.publishOnce("A", "a", discovery, vertx)
      .flatMap(v -> Service.publishOnce("A", "b", discovery, vertx))
      .flatMap(v -> Service.discover("A", discovery, vertx))
      .doOnTerminate(discovery::close)
      .subscribe(service -> {
        service.broadcast(data);
      }, context::fail);
  }
  
  /**
   * Test if a service can be published an unpublished again
   * @param context the test context
   */
  @Test
  public void unpublish(TestContext context) {
    Vertx vertx = new Vertx(rule.vertx());
    Async async = context.async();

    ServiceDiscovery discovery = ServiceDiscovery.create(vertx);
    Service.publishOnce("A", "a", discovery, vertx)
      .flatMap(v -> Service.discover("A", discovery, vertx))
      .count()
      .doOnNext(count -> {
        context.assertEquals(1, count);
      })
      .flatMap(v -> Service.discover("A", discovery, vertx))
      .flatMap(service -> service.unpublish(discovery))
      .flatMap(v -> Service.discover("A", discovery, vertx))
      .count()
      .doOnTerminate(discovery::close)
      .subscribe(count -> {
        context.assertEquals(0, count);
        async.complete();
      }, context::fail);
  }
}
