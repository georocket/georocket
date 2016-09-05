package io.georocket.service;

import io.vertx.servicediscovery.Record;
import io.vertx.servicediscovery.ServiceDiscovery;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.RunTestOnContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Test the implementation of {@link Service}
 *
 * @author Andrej Sajenko
 */
@RunWith(VertxUnitRunner.class)
public class ServiceTest {

  @Rule
  public RunTestOnContext rule = new RunTestOnContext();

  @Test
  public void testPublishOnce(TestContext context) {
    Vertx vertx = rule.vertx();
    Async async = context.async();

    Service.publishOnce(vertx, "A", "A").doOnError(context::fail).subscribe(v -> {
      Service.publishOnce(vertx, "A", "A").doOnError(context::fail).subscribe(v1 -> {
        Service.publishOnce(vertx, "A", "B").doOnError(context::fail).subscribe(v2 -> {
          ServiceDiscovery discovery = ServiceDiscovery.create(vertx);
          discovery.getRecords(record -> true, h -> {
            if (h.failed()) {
              async.complete();
              context.fail(h.cause());
            } else {
              List<Record> recordList = h.result();
              List<Record> lA = recordList.stream().filter(r -> r.getName().equals("A"))
                      .collect(Collectors.toList());

              context.assertEquals(2, lA.size());

              async.complete();
            }
          });
        });
      });
    });
  }

  @Test
  public void testDiscover(TestContext context) {
    Vertx vertx = rule.vertx();
    Async async = context.async();

    Service.publishOnce(vertx, "A", "a").doOnError(context::fail).subscribe(v -> {
      Service.publishOnce(vertx, "A", "b").doOnError(context::fail).subscribe(v1 -> {
        Service.discover(vertx, "A").doOnError(context::fail).count().subscribe(count -> {
          context.assertEquals(2, count);
          async.complete();
        });
      });
    });
  }

  @Test
  public void testSend(TestContext context) {
    Vertx vertx = rule.vertx();

    Async aC = context.async();
    Async bC = context.async();

    String data = "special data";

    vertx.eventBus().consumer("a", h -> {
      context.assertEquals(data, h.body());
      aC.complete();
    });
    vertx.eventBus().consumer("a", h -> {
      context.fail("Should not be called on send, only on broadcast");
    });
    vertx.eventBus().consumer("b", h -> {
      context.assertEquals(data, h.body());
      bC.complete();
    });

    Service.publishOnce(vertx, "A", "a").doOnError(context::fail).subscribe(v -> {
      Service.publishOnce(vertx, "A", "b").doOnError(context::fail).subscribe(v1 -> {
        Service.discover(vertx, "A").doOnError(context::fail).subscribe(service -> {
          service.send(data);
        });
      });
    });
  }

  @Test
  public void testBroadcast(TestContext context) {
    Vertx vertx = rule.vertx();

    Async aC = context.async();
    Async bC = context.async();

    String data = "special data";

    vertx.eventBus().consumer("a", h -> {
      context.assertEquals(data, h.body());
      aC.complete();
    });
    vertx.eventBus().consumer("a", h -> {
      context.assertEquals(data, h.body());
      bC.complete();
    });

    Service.publishOnce(vertx, "A", "a").doOnError(context::fail).subscribe(v -> {
        Service.discover(vertx, "A").doOnError(context::fail).subscribe(service -> {
          service.broadcast(data);
        });
    });
  }
}
