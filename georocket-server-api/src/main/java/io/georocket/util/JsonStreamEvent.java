package io.georocket.util;

/**
 * An event produced during JSON parsing
 * @since 1.0.0
 * @author Michel Kraemer
 */
public class JsonStreamEvent extends StreamEvent {
  private final int event;
  
  /**
   * Constructs a new event
   * @param event the actual JSON event type
   * @param pos the position in the JSON stream where the event has occurred
   */
  public JsonStreamEvent(int event, int pos) {
    super(pos);
    this.event = event;
  }
  
  /**
   * @return the actual JSON event type
   */
  public int getEvent() {
    return event;
  }
}
