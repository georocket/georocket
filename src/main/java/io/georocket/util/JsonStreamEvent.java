package io.georocket.util;

/**
 * An event produced during JSON parsing
 * @since 1.0.0
 * @author Michel Kraemer
 */
public class JsonStreamEvent extends StreamEvent {
  private final int event;
  private final Object currentValue;
  
  /**
   * Constructs a new event
   * @param event the actual JSON event type
   * @param pos the position in the JSON stream where the event has occurred
   */
  public JsonStreamEvent(int event, int pos) {
    this(event, pos, null);
  }
  
  /**
   * Constructs a new event
   * @param event the actual JSON event type
   * @param pos the position in the JSON stream where the event has occurred
   * @param currentValue the value or field name that was parsed when the event
   * was generated (may be <code>null</code>)
   */
  public JsonStreamEvent(int event, long pos, Object currentValue) {
    super(pos);
    this.event = event;
    this.currentValue = currentValue;
  }
  
  /**
   * @return the actual JSON event type
   */
  public int getEvent() {
    return event;
  }
  
  /**
   * @return the value or field name that was parsed when the event was
   * generated (may be <code>null</code>)
   */
  public Object getCurrentValue() {
    return currentValue;
  }
}
