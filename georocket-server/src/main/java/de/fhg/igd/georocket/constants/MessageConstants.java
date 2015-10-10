package de.fhg.igd.georocket.constants;

/**
 * Constants for messages sent over the event bus
 * @author Michel Kraemer
 */
public final class MessageConstants {
  public static final String ACTION = "action";
  public static final String ADD = "add";
  public static final String GET = "get";
  
  public static final String CHUNK = "chunk";
  public static final String NAME = "name";
  
  private MessageConstants() {
    // hidden constructor
  }
}
