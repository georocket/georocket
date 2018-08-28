package io.georocket.commands.console;

import io.georocket.util.DurationFormat;
import io.georocket.util.SizeFormat;
import io.vertx.core.Vertx;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.fusesource.jansi.Ansi;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;

import java.io.IOException;

import static org.fusesource.jansi.Ansi.ansi;

/**
 * Renders import progress to the terminal
 * @author Michel Kraemer
 */
public class ImportProgressRenderer {
  /**
   * Default refresh interval in milliseconds
   */
  private static final long DEFAULT_INTERVAL = 200;

  /**
   * Slower refresh interval in milliseconds (for dumb terminals)
   */
  private static final long DEFAULT_INTERVAL_SLOW = 1000;

  /**
   * Minimum terminal width. If the determined value is lower than this one
   * it will be set to {@link #DEFAULT_TERMINAL_WIDTH}
   */
  private static final int MIN_TERMINAL_WIDTH = 10;

  /**
   * Default terminal width (if the actual one could not be determined)
   */
  private static final int DEFAULT_TERMINAL_WIDTH = 80;

  /**
   * The current Vert.x instance
   */
  private final Vertx vertx;

  /**
   * ID of the refresh timer
   */
  private final long timerId;

  /**
   * The interval of the refresh timer
   */
  private final long interval;

  /**
   * The determined terminal width
   */
  private final int terminalWidth;

  /**
   * The time when the renderer was constructed
   */
  private final long startTime;

  /**
   * The name of the file currently being imported
   */
  private String filename = "Importing";

  /**
   * The total number of files to import
   */
  private int totalFiles = 0;

  /**
   * The index of the file currently being imported
   */
  private int index = 0;

  /**
   * The total size (in bytes) of the files to import
   */
  private long totalSize = 0;

  /**
   * The size (in bytes) of the file currently being imported
   */
  private long size = 0;

  /**
   * The number of bytes imported of the current file
   */
  private long current = 0;

  /**
   * The total number of bytes imported
   */
  private long totalProgress = 0;

  /**
   * The value of {@link #totalProgress} when {@link #render(boolean)} was
   * last called
   */
  private long lastTotalProgress = 0;

  /**
   * The last rendered output
   */
  private String lastRenderedOutput = "";

  /**
   * A sliding window to calculate the mean upload rate
   */
  private DescriptiveStatistics rateAvg = new DescriptiveStatistics(50);

  /**
   * Create a new renderer
   * @param interval the interval specifying how often the display should be
   * updated
   * @param terminalWidth the determined terminal width
   * @param vertx the current Vert.x instance
   */
  private ImportProgressRenderer(long interval, int terminalWidth, Vertx vertx) {
    this.vertx = vertx;
    this.terminalWidth = terminalWidth;
    this.startTime = System.currentTimeMillis();
    this.interval = interval;

    // render once and then start periodic timer
    render(false);
    timerId = vertx.setPeriodic(interval, l -> render(true));
  }

  /**
   * Create a new renderer
   * @param vertx the current Vert.x instance
   * @return the renderer
   */
  public static ImportProgressRenderer create(Vertx vertx) {
    // get capabilities of the terminal
    int terminalWidth;
    boolean dumb;
    try (Terminal terminal = TerminalBuilder.terminal()) {
      terminalWidth = terminal.getWidth();
      dumb = Terminal.TYPE_DUMB.equalsIgnoreCase(terminal.getType()) ||
        Terminal.TYPE_DUMB_COLOR.equalsIgnoreCase(terminal.getType());
    } catch (IOException e) {
      terminalWidth = DEFAULT_TERMINAL_WIDTH;
      dumb = true;
    }
    if (terminalWidth < MIN_TERMINAL_WIDTH) {
      terminalWidth = DEFAULT_TERMINAL_WIDTH;
    }

    // determine refresh interval
    long interval;
    if (dumb) {
      interval = DEFAULT_INTERVAL_SLOW;
    } else {
      interval = DEFAULT_INTERVAL;
    }

    // create renderer
    return new ImportProgressRenderer(interval, terminalWidth, vertx);
  }

  /**
   * Dispose this renderer and do not print progress anymore
   */
  public void dispose() {
    vertx.cancelTimer(timerId);
  }

  /**
   * Limit the given {@link StringBuilder} to the given length
   * @param builder the builder to limit
   * @param maxLength the maximum number of characters
   */
  private void limitToLength(StringBuilder builder, int maxLength) {
    if (builder.length() > maxLength) {
      builder.delete(maxLength, builder.length());
    }
  }

  /**
   * Renders the progress
   * @param resetCursor {@code true} if the console cursor should be reset
   * before the progress is rendered
   */
  private void render(boolean resetCursor) {
    // calculate upload rate
    long currentTotalProgress = totalProgress + current;
    rateAvg.addValue(currentTotalProgress - lastTotalProgress);
    long rate = Math.round(rateAvg.getSum() / (rateAvg.getN() / (1000.0  / interval)));
    lastTotalProgress = currentTotalProgress;

    // create first line consisting of filename and index
    StringBuilder line1 = new StringBuilder();
    line1.append(filename);

    if (index > 0 && totalFiles > 0) {
      line1.append(" (")
        .append(index)
        .append("/")
        .append(totalFiles)
        .append(")");
    }

    limitToLength(line1, terminalWidth - 4);

    // create second line displaying the progress in bytes
    StringBuilder line2 = new StringBuilder();
    line2.append("file ")
      .append(SizeFormat.format(current));
    if (size > 0 && totalSize > 0) {
      line2.append("/")
        .append(SizeFormat.format(size));
    }
    line2.append("  total ")
      .append(SizeFormat.format(currentTotalProgress));
    if (size > 0 && totalSize > 0) {
      line2.append("/")
        .append(SizeFormat.format(totalSize));
    }
    line2.append("  ")
      .append(SizeFormat.format(rate))
      .append("/s");

    limitToLength(line2, terminalWidth);

    // create third line indicating the elapsed time and the ETA
    long elapsed = System.currentTimeMillis() - startTime;
    StringBuilder line3 = new StringBuilder();
    line3.append("time ")
      .append(DurationFormat.formatShort(elapsed));
    if (rate > 0) {
      long remaining = elapsed + (totalSize - currentTotalProgress) / rate * 1000;
      line3.append("/")
        .append(DurationFormat.formatShort(remaining));
    }

    limitToLength(line3, terminalWidth);

    // output lines (if they have changed)
    Ansi ansi = ansi();
    if (resetCursor) {
      ansi.cursorUpLine(3);
    }
    ansi
      .eraseLine().a(line1).a(" ...").newline()
      .eraseLine().a(line2).newline()
      .eraseLine().a(line3).newline();

    String output = ansi.toString();
    if (!lastRenderedOutput.equals(output)) {
      System.out.print(output);
      System.out.flush();
    }
    lastRenderedOutput = output;
  }

  /**
   * Set the name of the file currently being imported and reset the
   * current file size and progress
   * @param filename the name
   * @return a reference to {@code this}
   */
  public ImportProgressRenderer startNewFile(String filename) {
    totalProgress += size;
    setCurrent(0);
    setSize(0);
    this.filename = filename;
    return this;
  }

  /**
   * Set the total number of files to be imported
   * @param totalFiles the total number of files
   * @return a reference to {@code this}
   */
  public ImportProgressRenderer setTotalFiles(int totalFiles) {
    this.totalFiles = totalFiles;
    return this;
  }

  /**
   * Set the index of the file currently being imported
   * @param index the index
   * @return a reference to {@code this}
   */
  public ImportProgressRenderer setIndex(int index) {
    this.index = index;
    return this;
  }

  /**
   * Set the total size of all files to be imported
   * @param totalSize the total file size
   * @return a reference to {@code this}
   */
  public ImportProgressRenderer setTotalSize(long totalSize) {
    this.totalSize = totalSize;
    return this;
  }

  /**
   * Set the size of the file currently being imported
   * @param size the file size
   * @return a reference to {@code this}
   */
  public ImportProgressRenderer setSize(long size) {
    this.size = size;
    return this;
  }

  /**
   * Set the current progress (i.e. the bytes already imported for the
   * current file)
   * @param current the current progress
   * @return a reference to {@code this}
   */
  public ImportProgressRenderer setCurrent(long current) {
    this.current = current;
    return this;
  }
}
