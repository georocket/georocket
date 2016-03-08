package io.georocket.storage.file;

import io.georocket.constants.ConfigConstants;
import io.georocket.storage.StorageTest;
import io.georocket.storage.Store;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.TestContext;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

/**
 * Test {@link FileStore}
 * @author Andrej Sajenko
 */
public class FileStoreTest extends StorageTest {

  /**
   * Create a temporary folder
   */
  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  protected Path storagePath;

  /**
   * Set up test dependencies.
   */
  @Before
  public void setUp() throws Exception {
    storagePath = Paths.get(folder.getRoot().getAbsolutePath(), "storage");
  }

  private void configureVertx(Vertx vertx) {
    vertx.getOrCreateContext().config().put(ConfigConstants.STORAGE_FILE_PATH, storagePath.toString());
  }

  @Override
  protected Store createStore(Vertx vertx) {
    this.configureVertx(vertx);
    return new FileStore(vertx);
  }

  @Override
  protected Handler<Future<String>> prepare_Data(TestContext context, Vertx vertx, String subPath) {
    return h -> {
      Path fileFolder = Paths.get(storagePath.toString(), "file");
      Path fileDestinationFolder = subPath == null || subPath.isEmpty() ? fileFolder : Paths.get(fileFolder.toString(), subPath);

      Path filePath = Paths.get(fileDestinationFolder.toString(), id);

      try {
        Files.createDirectories(fileDestinationFolder);
        Files.write(filePath, chunkContent.getBytes());
      } catch (IOException ex) {
        context.fail("Failed to create test files: " + ex.getMessage());
      }

      h.complete(filePath.toString().replace(fileFolder.toString() + "/", ""));
    };
  }

  @Override
  protected Handler<Future<Object>> validate_after_Store_add(TestContext context, Vertx vertx, String path) {
    return h -> {
      Path root = Paths.get(storagePath.toString(), "/file");
      root = (path == null || path.isEmpty()) ? root : Paths.get(root.toString(), path);

      File folder = root.toFile();

      if (!folder.exists()) {
        context.fail("FileStore did not wrote a folder: " + folder.getAbsolutePath());
      }

      File[] files = folder.listFiles();

      if (files.length == 0) {
        context.fail("FileStore did not wrote a file in: " + folder.getAbsolutePath());
      }

      File first = files[0];

      try {
        List<String> lines = Files.readAllLines(first.toPath());

        if (lines.isEmpty()) {
          context.fail("FileStore did not wrote any content in file: " + first.getAbsolutePath());
        }

        String firstLine = lines.get(0);
        context.assertEquals(chunkContent, firstLine);

      } catch (IOException ex) {
        context.fail("Could not read the file: " + ex.getMessage());
      }

      h.complete();
    };
  }

  @Override
  protected Handler<Future<Object>> validate_after_Store_delete(TestContext context, Vertx vertx, String path) {
    return h -> {
      if (Files.exists(Paths.get(path))) {
        context.fail("Test expected to find zero files after calling HDFSStore::delete. FilePath('" + path + "')");
      }

      h.complete();
    };
  }
}
