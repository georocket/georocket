package io.georocket.util;

import org.apache.tika.Tika;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.spi.FileTypeDetector;

/**
 * <p>File type detector that use the apache tika detector.</p>
 *
 * <p>This type detector use the magic number and file extension to guess the right content type.</p>
 *
 * <p>See {@link Tika#detect(Path)}</p>
 *
 * @author Andrej Sajenko
 */
public class TikaFileTypeDetector extends FileTypeDetector {

  private Tika tika = new Tika();

  @Override
  public String probeContentType(Path path) throws IOException {
    return tika.detect(path);
  }
}
