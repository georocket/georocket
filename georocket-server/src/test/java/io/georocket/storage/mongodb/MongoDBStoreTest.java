package io.georocket.storage.mongodb;

import de.bwaldvogel.mongo.MongoServer;
import de.bwaldvogel.mongo.backend.memory.MemoryBackend;
import io.georocket.constants.ConfigConstants;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.RunTestOnContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.net.InetSocketAddress;

import static org.junit.Assert.*;

/**
 * Test {@link MongoDBStore}
 * @author Andrej Sajenko
 */
@RunWith(VertxUnitRunner.class)
public class MongoDBStoreTest {

  @Rule
  public RunTestOnContext rule = new RunTestOnContext();

  private MongoServer server;
  InetSocketAddress serverAddress;

  @Before
  public void setUp() {
    server = new MongoServer(new MemoryBackend());
    serverAddress = server.bind();
  }

  @After
  public void tearDown() {
    server.shutdown();
  }


  @Test
  public void testGetOne(TestContext context) throws Exception {
    Vertx vertx = rule.vertx();
    JsonObject config = vertx.getOrCreateContext().config();

    //config.put(ConfigConstants.STORAGE_MONGODB_HOST, serverAddress)


    context.fail("Test not implemented");
  }

  @Test
  public void testAdd(TestContext context) throws Exception {
    context.fail("Test not implemented");
  }

  @Test
  public void testDelete(TestContext context) throws Exception {
    context.fail("Test not implemented");
  }

  @Test
  public void testGet(TestContext context) throws Exception {
    context.fail("Test not implemented");
  }
}