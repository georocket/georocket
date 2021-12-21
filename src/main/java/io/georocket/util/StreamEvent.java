package io.georocket.util;

/**
 * An event produced during input parsing
 * @since 1.0.0
 * @author Michel Kraemer
 */
public class StreamEvent {
  private final long pos;

  /**
   * Constructs a new event
   * @param pos the position in the input stream where the event has occurred
   */
  public StreamEvent(long pos) {
    this.pos = pos;
  }

  /**
   * @return the position in the input stream where the event has occurred
   */
  public long getPos() {
    return pos;
  }
}
