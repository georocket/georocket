package io.georocket.storage.file;

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
import io.vertx.ext.unit.TestContext;

/**
 * Test {@link FileStore}
 * @author Andrej Sajenko
 */
public class FileStoreTest extends StorageTest {
  /**
   * Create a temporary tempFolder
   */
  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  private String fileStoreRoot;
  private String fileDestination;

  /**
   * Set up test dependencies.
   */
  @Before
  public void setUp() {
    fileStoreRoot = tempFolder.getRoot().getAbsolutePath();
    fileDestination = PathUtils.join(fileStoreRoot, "file");
  }

  private void configureVertx(Vertx vertx) {
    vertx.getOrCreateContext().config().put(ConfigConstants.STORAGE_FILE_PATH, fileStoreRoot);
  }

  @Override
  protected Store createStore(Vertx vertx) {
    configureVertx(vertx);
    return new FileStore(vertx);
  }

  @Override
  protected void prepareData(TestContext context, Vertx vertx, String subPath,
      Handler<AsyncResult<String>> handler) {
    vertx.executeBlocking(f -> {
      String destinationFolder = subPath == null || subPath.isEmpty() ?
          fileDestination : PathUtils.join(fileDestination, subPath);
      Path filePath = Paths.get(destinationFolder.toString(), ID);
      try {
        Files.createDirectories(Paths.get(destinationFolder));
        Files.write(filePath, CHUNK_CONTENT.getBytes());
        f.complete(filePath.toString().replace(fileDestination + "/", ""));
      } catch (IOException ex) {
        f.fail(ex);
      }
    }, handler);
  }

  @Override
  protected void validateAfterStoreAdd(TestContext context, Vertx vertx,
      String path, Handler<AsyncResult<Void>> handler) {
    vertx.executeBlocking(f -> {
      String destinationFolder = path == null || path.isEmpty() ?
          fileDestination : PathUtils.join(fileDestination, path);
      
      File folder = new File(destinationFolder);
      context.assertTrue(folder.exists());

      File[] files = folder.listFiles();
      context.assertNotNull(files);
      context.assertEquals(1, files.length);
      File file = files[0];

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
