package io.georocket.storage.file;

import io.georocket.constants.ConfigConstants;
import io.georocket.storage.StorageTest;
import io.georocket.storage.Store;
import io.georocket.util.PathUtils;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.TestContext;
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
public class FileStoreTestImpl extends StorageTest {

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  private Path storagePath;


  @Override
  public void setUp(TestContext context) {
    storagePath = Paths.get(folder.getRoot().getAbsolutePath(), "storage");
  }

  @Override
  public void tearDown() {

  }

  @Override
  protected void configureVertx(Vertx vertx) {
    vertx.getOrCreateContext().config().put(ConfigConstants.STORAGE_FILE_PATH, storagePath.toString());
  }

  @Override
  protected Store createStore(Vertx vertx) {
    return new FileStore(vertx);
  }

  @Override
  protected Handler<Future<String>> prepare_Data(TestContext context, Vertx vertx, String subPath) {
    return h -> {
      Path fileDestinationFolder = Paths.get(storagePath.toString(), "file");
      fileDestinationFolder = subPath == null || subPath.isEmpty() ? fileDestinationFolder : Paths.get(fileDestinationFolder.toString(), subPath);

      Path filePath = Paths.get(fileDestinationFolder.toString(), id);

      try {
        Files.createDirectories(fileDestinationFolder);
        Files.write(filePath, chunkContent.getBytes());
      } catch (IOException ex) {
        context.fail("Failed to create test files: " + ex.getMessage());
      }

      h.complete(filePath.toString().replace(fileDestinationFolder.toString() + "/", ""));
    };
  }

  @Override
  protected Handler<Future<Object>> validate_after_Store_add(TestContext context, Vertx vertx, String path) {
    return h -> {
      Path root = Paths.get(storagePath.toString(), "/file");
      root = (path == null || path.isEmpty()) ? root : Paths.get(root.toString(), path);

      final File folder = root.toFile();
      if (!folder.exists()) context.fail("FileStore did not wrote a folder: " + folder.getAbsolutePath());
      File[] files = folder.listFiles();

      if (files.length == 0) context.fail("FileStore did not wrote a file in: " + folder.getAbsolutePath());
      final File first = files[0];

      try {
        final List<String> lines = Files.readAllLines(first.toPath());
        if (lines.isEmpty()) context.fail("FileStore did not wrote any content in file: " + first.getAbsolutePath());

        final String firstLine = lines.get(0);
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
      if (Files.exists(Paths.get(path))) context.fail("File with chunk's should be deleted on Storage::delete");
      h.complete();
    };
  }
}
