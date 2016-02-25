package io.georocket.storage.file;

import io.georocket.constants.AddressConstants;
import io.georocket.constants.ConfigConstants;
import io.georocket.storage.ChunkMeta;
import io.georocket.storage.ChunkReadStream;
import io.georocket.storage.StoreCursor;
import io.georocket.util.XMLStartElement;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.RunTestOnContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.bson.types.ObjectId;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

/**
 * Test {@link FileStore}
 * @author Andrej Sajenko
 */
@RunWith(VertxUnitRunner.class)
public class FileStoreTest {

  /**
   * Run the test on a Vert.x test context
   */
  @Rule
  public RunTestOnContext rule = new RunTestOnContext();

  /**
   * Create a temporary folder
   */
  @Rule
  public TemporaryFolder folder = new TemporaryFolder();


  private Path storagePath;

  @Before
  public void setUp() {
    storagePath = Paths.get(folder.getRoot().getAbsolutePath(), "storage");
  }

  private static String chunkContent = "<b>This is a chunk content</b>";
  private static String search = "irrelevant but necessary value"; // value is irrelevant for the test, because this test do not use the Indexer

  private static JsonObject indexerQueryReplyMsg;

  private static String id = new ObjectId().toString();
  private static JsonArray parents = new JsonArray();
  private static int start = 0;
  private static int end = 5;
  private static Long totalHits = 1L;
  private static String scrollId = "0";
  static {
    JsonArray hits = new JsonArray();
    JsonObject hit = new JsonObject()
        .put("parents", parents)
        .put("start", start)
        .put("end", end)
        .put("id", id);

    hits.add(hit);

    indexerQueryReplyMsg = new JsonObject()
        .put("totalHits", totalHits)
        .put("scrollId", scrollId)
        .put("hits", hits);
  }

  private void registerIndexerQueryConsumer(Vertx vertx, TestContext context, Async async) {
    vertx.eventBus().<JsonObject>consumer(AddressConstants.INDEXER_QUERY).handler(request -> {
      JsonObject msg = request.body();

      if (!msg.containsKey("pageSize")) context.fail("Malformed Message: expected to have 'pageSize' attribute");
      int pageSize = msg.getInteger("pageSize"); // pageSize == IndexStore.PAGE_SIZE | msg need this attribute

      if (!msg.containsKey("search")) context.fail("Malformed Message: expected to have 'search' attribute");
      String indxSearch = msg.getString("search");

      context.assertEquals(search, indxSearch);

      request.reply(indexerQueryReplyMsg);

      async.complete();
    });
  }

  private Path createFileWithContent(TestContext context, String id, String content, String subPath) {

    Path fileDestinationFolder = Paths.get(storagePath.toString(), "file");
        fileDestinationFolder = subPath == null || subPath.isEmpty() ? fileDestinationFolder : Paths.get(fileDestinationFolder.toString(), subPath);

    Path filePath = Paths.get(fileDestinationFolder.toString(), id);

    try {
      Files.createDirectories(fileDestinationFolder);
      Files.write(filePath, content.getBytes());
    } catch (IOException ex) {
      context.fail("Failed to create test files: " + ex.getMessage());
    }

    return filePath;
  }
  private Path createFileWithContent(TestContext context, String id, String content) {
    return this.createFileWithContent(context, id, content, "");
  }

  private void setConfig(Vertx vertx) {
    vertx.getOrCreateContext().config().put(ConfigConstants.STORAGE_FILE_PATH, storagePath.toString());
  }


  @Test
  public void testGetOne(TestContext context) throws Exception {
    Vertx vertx = rule.vertx();
    Async asyncGetOne = context.async();

    Path filePath = this.createFileWithContent(context, id, chunkContent);

    this.setConfig(vertx);

    FileStore fileStore = new FileStore(vertx);

    fileStore.getOne(id, h -> {
      ChunkReadStream chunkReadStream = h.result();

      chunkReadStream.handler(buffer -> {
        String receivedChunk = new String(buffer.getBytes());
        context.assertEquals(chunkContent, receivedChunk);
      }).endHandler( end -> {
        asyncGetOne.complete();
      });
    });
  }

  @Test
  public void testAdd(TestContext context) throws Exception {
    this.testAddHelper(context, null); // without path
  }

  @Test
  public void testAddWithoutSubFolder(TestContext context) throws Exception {
    this.testAddHelper(context, "subFolder"); // with path
  }

  public void testAddHelper(TestContext context, String path) throws Exception {
    String chunk = "<b>This is a test chunk</b>";
    String XMLHEADER = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n";
    String xml = XMLHEADER + "<root>\n<object><child></child></object>\n</root>";
    ChunkMeta meta = new ChunkMeta(Arrays.asList(new XMLStartElement("root")), XMLHEADER.length() + 7, xml.length() - 8);

    List tags = Arrays.asList("a", "b", "c");

    Vertx vertx = rule.vertx();
    Async asyncIndexerAdd = context.async();
    Async asyncAdd = context.async();

    this.setConfig(vertx);

    FileStore fileStore = new FileStore(vertx);

    vertx.eventBus().<JsonObject>consumer(AddressConstants.INDEXER_ADD).handler(handler -> {
      JsonObject index = handler.body();

      context.assertEquals(meta.toJsonObject(), index.getJsonObject("meta"));
      context.assertEquals(new JsonArray(tags), index.getJsonArray("tags"));

      asyncIndexerAdd.complete();
    });

    // register delete
    vertx.eventBus().<JsonObject>consumer(AddressConstants.INDEXER_DELETE).handler(h -> context.fail("Indexer should not be notified for delete on add of a store!"));
    // register query
    vertx.eventBus().<JsonObject>consumer(AddressConstants.INDEXER_QUERY).handler(h -> context.fail("Indexer should not be notified for query on add of a store!"));

    fileStore.add(chunk, meta, path, tags, context.asyncAssertSuccess(err -> {
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
        context.assertEquals(chunk, firstLine);

        asyncAdd.complete();
      } catch (IOException ex) {
        context.fail("Could not read the file: " + ex.getMessage());
      }
    }));
  }

  @Test
  public void testDelete(TestContext context) throws Exception {
    this.testDeleteHelper(context, null);
  }

  @Test
  public void testDeleteWithSubfolder(TestContext context) throws Exception {
    this.testDeleteHelper(context, "subFolderA/subFolderB");
  }

  public void testDeleteHelper(TestContext context, String path) throws Exception {
    final Vertx vertx = rule.vertx();
    final Async asyncIndexerQuery = context.async();
    final Async asyncIndexerDelete = context.async();
    final Async asyncDelete = context.async();

    this.setConfig(vertx);
    Path filePath = this.createFileWithContent(context, id, chunkContent, path);
    Path fileDestinationFolder = filePath.getParent();

    FileStore fileStore = new FileStore(vertx);

    // register add
    vertx.eventBus().<JsonObject>consumer(AddressConstants.INDEXER_ADD).handler(h -> context.fail("Indexer should not be notified on delete of a store!"));
    // register delete
    vertx.eventBus().<JsonObject>consumer(AddressConstants.INDEXER_DELETE).handler(req -> {
      JsonObject msg = req.body();

      if(!msg.containsKey("paths")) context.fail("Malformed Message: expected to have 'pageSize' attribute");
      JsonArray paths = msg.getJsonArray("paths");

      if(paths.size() != 1) context.fail("Expected to find exact one path in MSG, found: " + paths.size());

      context.assertEquals(filePath, Paths.get(fileDestinationFolder.toString(), paths.getString(0)));

      req.reply(null); // Value is not used in Store

      asyncIndexerDelete.complete();
    });

    // register query
    this.registerIndexerQueryConsumer(vertx, context, asyncIndexerQuery);

    fileStore.delete(search, path, context.asyncAssertSuccess(h -> {

      if (Files.exists(filePath)) context.fail("File with chunk's should be deleted on Storage::delete");

      asyncDelete.complete();
    }));
  }

  @Test
  public void testGet(TestContext context) throws Exception {
    this.testGetHelper(context, null);
  }

  @Test
  public void testGetWithSubfolder(TestContext context) throws Exception {
    this.testGetHelper(context, "subFolder");
  }

  public void testGetHelper(TestContext context, String path) throws Exception {
    final Vertx vertx = rule.vertx();
    final Async asyncQuery = context.async();
    final Async asyncGet = context.async();

    Path filePath = this.createFileWithContent(context, id, chunkContent, path);
    Path fileDestinationFolder = filePath.getParent();
    this.setConfig(vertx);

    FileStore fileStore = new FileStore(vertx);


    // register query
    this.registerIndexerQueryConsumer(vertx, context, asyncQuery);

    fileStore.get(search, path, ar -> {
      StoreCursor cursor = ar.result();

      if (!cursor.hasNext()) context.fail("Cursor is empty: Expected to have one element.");
      cursor.next(h -> {
        ChunkMeta meta = h.result();

        context.assertEquals(end, meta.getEnd());
        // context.assertEquals(parents, meta.getParents());
        context.assertEquals(start, meta.getStart());

        String fileName = cursor.getChunkPath();

        context.assertEquals(id, fileName);

        Path expectedFilePath = Paths.get(fileDestinationFolder.toString(), fileName);


        if (!Files.exists(expectedFilePath)) context.fail("File '" + expectedFilePath.toString() + "' expected but not found.");

        try {
          final List<String> lines = Files.readAllLines(expectedFilePath);
          if (lines.isEmpty()) context.fail("Found an empty file: " + expectedFilePath.toString());

          final String firstLine = lines.get(0);
          context.assertEquals(chunkContent, firstLine);
        } catch (IOException ex) {
          context.fail("Could not read the file. " + ex.getMessage());
        }

        asyncGet.complete();
      });
    });
  }
}