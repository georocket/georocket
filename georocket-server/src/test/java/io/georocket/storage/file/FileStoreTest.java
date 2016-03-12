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
import io.vertx.core.Future;
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
    this.configureVertx(vertx);
    return new FileStore(vertx);
  }

  @Override
  protected Handler<Future<String>> prepareData(TestContext context, Vertx vertx, String subPath) {
    return h -> {
      String destinationFolder = subPath == null || subPath.isEmpty() ?
          fileDestination : PathUtils.join(fileDestination, subPath);

      Path filePath = Paths.get(destinationFolder.toString(), ID);

      try {
        Files.createDirectories(Paths.get(destinationFolder));
        Files.write(filePath, CHUNK_CONTENT.getBytes());
      } catch (IOException ex) {
        context.fail("Test file creation failed.: " + ex.getMessage());
      }

      h.complete(filePath.toString().replace(fileDestination + "/", ""));
    };
  }

  @Override
  protected Handler<Future<Object>> validateAfterStoreAdd(TestContext context, Vertx vertx, String path) {
    return h -> {
      String destinationFolder = path == null || path.isEmpty() ?
          fileDestination : PathUtils.join(fileDestination, path);

      File folder = new File(destinationFolder);

      if (!folder.exists()) {
        context.fail("Test expected to find a folder after calling "
            + "FileStore::add. FolderPath('" + folder.getAbsolutePath() + "')");
      }

      File[] files = folder.listFiles();

      if (files == null || files.length != 1) {
        context.fail("Test expected to find one file after calling "
            + "FileStore::add FolderPath('" + folder.getAbsolutePath() + "')");
      }

      File file = files[0];

      try {
        List<String> lines = Files.readAllLines(file.toPath());

        if (lines.isEmpty()) {
          context.fail("Test expected to find any content after calling "
              + "FileStore::add FilePath('" + file.getAbsolutePath() + "')");
        }

        String firstLine = lines.get(0);

        context.assertEquals(CHUNK_CONTENT, firstLine);

      } catch (IOException ex) {
        context.fail("Could not read a file where the test expected to find "
            + "one, after calling FileStore::add.  ExMsg: " + ex.getMessage());
      }

      h.complete();
    };
  }

  @Override
  protected Handler<Future<Object>> validateAfterStoreDelete(TestContext context,
      Vertx vertx, String path) {
    return h -> {
      if (Files.exists(Paths.get(path))) {
        context.fail("Test expected to find zero files after calling "
            + "FileStore::delete. FilePath('" + path + "')");
      }

      h.complete();
    };
  }
}
