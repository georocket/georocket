package io.georocket.storage.s3;

import com.amazonaws.HttpMethod;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.S3ClientOptions;
import io.georocket.constants.ConfigConstants;
import io.georocket.storage.ChunkReadStream;
import io.georocket.storage.indexed.IndexedStore;
import io.georocket.util.AsyncXMLParser;
import io.georocket.util.PathUtils;
import io.georocket.util.XMLStreamEvent;
import io.georocket.util.io.DelegateChunkReadStream;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.bson.types.ObjectId;
import rx.Observable;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.events.XMLEvent;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Queue;

/**
 * Stores chunks on Amazon S3
 * @author Michel Kraemer
 */
public class S3Store extends IndexedStore {
  private static Logger log = LoggerFactory.getLogger(S3Store.class);

  private final Vertx vertx;
  private AmazonS3Client s3Client;
  private final String accessKey;
  private final String secretKey;
  private final String host;
  private final int port;
  private final String bucket;
  private final boolean pathStyleAccess;
  private final HttpClient client;

  /**
   * Constructs a new store
   * @param vertx the Vert.x instance
   */
  public S3Store(Vertx vertx) {
    super(vertx);
    this.vertx = vertx;

    JsonObject config = vertx.getOrCreateContext().config();
    accessKey = config.getString(ConfigConstants.STORAGE_S3_ACCESS_KEY);
    secretKey = config.getString(ConfigConstants.STORAGE_S3_SECRET_KEY);
    host = config.getString(ConfigConstants.STORAGE_S3_HOST);
    port = config.getInteger(ConfigConstants.STORAGE_S3_PORT, 80);
    bucket = config.getString(ConfigConstants.STORAGE_S3_BUCKET);
    pathStyleAccess = config.getBoolean(ConfigConstants.STORAGE_S3_PATH_STYLE_ACCESS, true);

    HttpClientOptions options = new HttpClientOptions();
    options.setDefaultHost(host);
    options.setDefaultPort(port);
    client = vertx.createHttpClient(options);
  }

  /**
   * Get or initialize the S3 client.
   * Note: this method must be synchronized because we're accessing the
   * {@link #s3Client} field and we're calling this method from a worker thread.
   * @return the S3 client
   */
  private synchronized AmazonS3Client getS3Client() {
    if (s3Client == null) {
      BasicAWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);
      s3Client = new AmazonS3Client(credentials);
      s3Client.setEndpoint("http://" + host + ":" + port);
      s3Client.setS3ClientOptions(new S3ClientOptions().withPathStyleAccess(pathStyleAccess));
    }
    return s3Client;
  }

  /**
   * Generate a pre-signed URL that can be used to make an HTTP request.
   * Note: this method must be synchronized because we're accessing the
   * {@link #s3Client} field and we're calling this method from a worker thread.
   * @param key the key of the S3 object to query
   * @param method the HTTP method that will be used in the request
   * @return the presigned URL
   */
  private synchronized URL generatePresignedUrl(String key, HttpMethod method) {
    return getS3Client().generatePresignedUrl(bucket, key, null, method);
  }

  @Override
  protected void doAddChunk(String chunk, String path, Handler<AsyncResult<String>> handler) {
    if (path == null || path.isEmpty()) {
      path = "/";
    }

    // generate new file name
    String id = new ObjectId().toString();
    String filename = PathUtils.join(path, id);
    String key = PathUtils.removeLeadingSlash(filename);

    vertx.<URL>executeBlocking(f -> {
      f.complete(generatePresignedUrl(key, HttpMethod.PUT));
    }, ar -> {
      if (ar.failed()) {
        handler.handle(Future.failedFuture(ar.cause()));
        return;
      }

      URL u = ar.result();
      log.debug("PUT " + u);

      Buffer chunkBuf = Buffer.buffer(chunk);
      HttpClientRequest request = client.put(u.getFile());

      request.putHeader("Host", u.getHost());
      request.putHeader("Content-Length", String.valueOf(chunkBuf.length()));

      request.exceptionHandler(t -> {
        handler.handle(Future.failedFuture(t));
      });

      request.handler(response -> {
        Buffer errorBody = Buffer.buffer();
        if (response.statusCode() != 200) {
          response.handler(buf -> {
            errorBody.appendBuffer(buf);
          });
        }
        response.endHandler(v -> {
          if (response.statusCode() == 200) {
            handler.handle(Future.succeededFuture(filename));
          } else {
            log.error(errorBody);
            handler.handle(Future.failedFuture(response.statusMessage()));
          }
        });
      });

      request.end(chunkBuf);
    });
  }

  @Override
  public void getOne(String path, Handler<AsyncResult<ChunkReadStream>> handler) {
    String key = PathUtils.removeLeadingSlash(PathUtils.normalize(path));
    vertx.<URL>executeBlocking(f -> {
      f.complete(generatePresignedUrl(key, HttpMethod.GET));
    }, ar -> {
      if (ar.failed()) {
        handler.handle(Future.failedFuture(ar.cause()));
        return;
      }

      URL u = ar.result();
      log.debug("GET " + u);

      HttpClientRequest request = client.get(ar.result().getFile());
      request.putHeader("Host", u.getHost());

      request.exceptionHandler(t -> {
        handler.handle(Future.failedFuture(t));
      });

      request.handler(response -> {
        if (response.statusCode() == 200) {
          String contentLength = response.getHeader("Content-Length");
          long chunkSize = Long.parseLong(contentLength);
          handler.handle(Future.succeededFuture(new DelegateChunkReadStream(chunkSize, response)));
        } else {
          Buffer errorBody = Buffer.buffer();
          response.handler(buf -> {
            errorBody.appendBuffer(buf);
          });
          response.endHandler(v -> {
            log.error(errorBody);
            handler.handle(Future.failedFuture(response.statusMessage()));
          });
        }
      });

      request.end();
    });
  }

  public static URI appendUri(String uri, String appendQuery) throws URISyntaxException {
    URI oldUri = new URI(uri);

    String newQuery = oldUri.getQuery();
    if (newQuery == null) {
      newQuery = appendQuery;
    } else {
      newQuery += "&" + appendQuery;
    }

    URI newUri = new URI(oldUri.getScheme(), oldUri.getAuthority(),
        oldUri.getPath(), newQuery, oldUri.getFragment());

    return newUri;
  }

  private URL prepandQuery(URL source, String query) throws URISyntaxException, MalformedURLException {
    URI oldUri = source.toURI();

    String queryPart = oldUri.getQuery();
    if (queryPart == null) {
      queryPart = query;
    } else {
      queryPart = query + "&" + queryPart;
    }

    URI result = new URI(oldUri.getScheme(), oldUri.getAuthority(), oldUri.getPath(), queryPart, oldUri.getFragment());

    return result.toURL();
  }

  @Override
  public void getStoredSize(Handler<AsyncResult<Long>> handler) {
    // http://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketGET.html
    vertx.<URL>executeBlocking(f -> {
      URL url = generatePresignedUrl("", HttpMethod.GET);
      try {
        url = prepandQuery(url, "list-type=2");
        f.complete(url);
      } catch (Exception ex) {
        log.fatal("Got a Malformed amazon s3 url");
        f.fail(ex);
      }
    }, ar -> {
      if (ar.failed()) {
        handler.handle(Future.failedFuture(ar.cause()));
        return;
      }
      URL url = ar.result();
      log.debug("GET " + url);

      HttpClientRequest request = this.client.get(url.getFile());
      request.putHeader("Host", url.getHost());

      request.exceptionHandler(ex -> {
        handler.handle(Future.failedFuture(ex));
      });

      request.handler(response -> {
        if (response.statusCode() == 200) {
          String contentType = response.getHeader("Content-Type");

          if (contentType.contains("application/xml")) {
            Buffer bodyBuffer = Buffer.buffer();

            response.handler(bodyBuffer::appendBuffer).endHandler( v -> {
              AsyncXMLParser parser = new AsyncXMLParser();

              Observable<XMLStreamEvent> sizeElements = parser.feed(bodyBuffer).filter((xmlEvent) -> {
                XMLStreamReader reader = xmlEvent.getXMLReader();

                boolean isStartEvent = xmlEvent.getEvent() == XMLEvent.START_ELEMENT;

                return isStartEvent && "Size".equals(reader.getLocalName());
              });

              Observable<Long> sizes = sizeElements.map(e -> {
                try {
                  XMLStreamReader reader = e.getXMLReader();
                  String sizeAsText = reader.getElementText();

                  return Long.parseLong(sizeAsText);
                } catch (XMLStreamException ex) {
                  log.warn("S3: Expected to find a element with text content only! Will continue with '0' as size for this element");
                  return 0L;
                }
              });

              Observable<Long> size = sizes.reduce(0L, (a, b) -> a + b);

              size.subscribe(l -> handler.handle(Future.succeededFuture(l)));

            });

          } else {
            handler.handle(Future.failedFuture("Expected xml as result."));
          }
        } else {
          Buffer errorBody = Buffer.buffer();
          response
              .handler(errorBody::appendBuffer)
              .endHandler(v -> {
                log.error(errorBody);
                handler.handle(Future.failedFuture(response.statusMessage()));
              });
        }
      });

      request.end();
    });
  }

  @Override
  protected void doDeleteChunks(Queue<String> paths, Handler<AsyncResult<Void>> handler) {
    if (paths.isEmpty()) {
      handler.handle(Future.succeededFuture());
      return;
    }

    String key = PathUtils.removeLeadingSlash(PathUtils.normalize(paths.poll()));
    vertx.<URL>executeBlocking(f -> {
      f.complete(generatePresignedUrl(key, HttpMethod.DELETE));
    }, ar -> {
      if (ar.failed()) {
        handler.handle(Future.failedFuture(ar.cause()));
        return;
      }

      URL u = ar.result();
      log.debug("DELETE " + u);

      HttpClientRequest request = client.delete(ar.result().getFile());
      request.putHeader("Host", u.getHost());

      request.exceptionHandler(t -> {
        handler.handle(Future.failedFuture(t));
      });

      request.handler(response -> {
        Buffer errorBody = Buffer.buffer();
        if (response.statusCode() != 204) {
          response.handler(buf -> {
            errorBody.appendBuffer(buf);
          });
        }
        response.endHandler(v -> {
          switch (response.statusCode()) {
            case 204:
            case 404:
              doDeleteChunks(paths, handler);
              break;
            default:
              log.error(errorBody);
              handler.handle(Future.failedFuture(response.statusMessage()));
          }
        });
      });

      request.end();
    });
  }
}
