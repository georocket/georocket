package io.georocket.storage.hdfs;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import io.georocket.constants.ConfigConstants;
import io.georocket.storage.StorageTest;
import io.georocket.storage.Store;
import io.georocket.util.PathUtils;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;

/**
 * Test {@link HDFSStore}
 * @author Andrej Sajenko
 */
public class HDFSStoreTest extends StorageTest {
  /**
   * Create a temporary folder
   */
  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  private String hdfsLocalRoot;
  private String hdfsAdress;

  /**
   * Set up test dependencies.
   */
  @Before
  public void setUp() {
    hdfsLocalRoot = tempFolder.getRoot().getAbsolutePath();
    hdfsAdress = "file://" + hdfsLocalRoot;
  }

  private void configureVertx(Vertx vertx) {
    JsonObject config = vertx.getOrCreateContext().config();

    // prevent exception -> http://stackoverflow.com/questions/19840056/failed-to-detect-a-valid-hadoop-home-directory
    System.setProperty("hadoop.home.dir", "/");

    config.put(ConfigConstants.STORAGE_HDFS_PATH, hdfsLocalRoot);
    config.put(ConfigConstants.STORAGE_HDFS_DEFAULT_FS, hdfsAdress);
  }

  @Override
  protected Store createStore(Vertx vertx) {
    configureVertx(vertx);
    return new HDFSStore(vertx);
  }

  @Override
  protected void prepareData(TestContext context, Vertx vertx, String path,
      Handler<AsyncResult<String>> handler) {
    vertx.executeBlocking(f -> {
      String destinationFolder = path == null || path.isEmpty() ?
          hdfsLocalRoot : PathUtils.join(hdfsLocalRoot, path);
      Path filePath = Paths.get(destinationFolder, ID);
      try {
        Files.createDirectories(Paths.get(destinationFolder));
        Files.write(filePath, CHUNK_CONTENT.getBytes());
        f.complete(filePath.toString().replace(hdfsLocalRoot + "/", ""));
      } catch (IOException ex) {
        f.fail(ex);
      }
    }, handler);
  }

  @Override
  protected void validateAfterStoreAdd(TestContext context, Vertx vertx,
      String path, Handler<AsyncResult<Void>> handler) {
    vertx.executeBlocking(f -> {
      String fileDestination = (path == null || path.isEmpty()) ?
          hdfsLocalRoot : PathUtils.join(hdfsLocalRoot, path);

      File folder = new File(fileDestination);
      context.assertTrue(folder.exists());

      File[] files = folder.listFiles();
      context.assertNotNull(files);
      context.assertEquals(2, files.length);

      // Hadoop client creates two files, one starts with a point '.' and ends
      // with the extension ".crc". The other file contains the needed content.
      File file = files[0].getPath().endsWith(".crc") ? files[1] : files[0];

      List<String> lines;
      try {
        lines = Files.readAllLines(file.toPath());
      } catch (IOException ex) {
        f.fail(ex);
        return;
      }
      
      context.assertFalse(lines.isEmpty());
      String firstLine = lines.get(0);
      context.assertEquals(CHUNK_CONTENT, firstLine);
      
      f.complete();
    }, handler);
  }

  @Override
  protected void validateAfterStoreDelete(TestContext context,
      Vertx vertx, String path, Handler<AsyncResult<Void>> handler) {
    vertx.executeBlocking(f -> {
      context.assertFalse(Files.exists(Paths.get(path)));
      f.complete();
    }, handler);
  }
}
