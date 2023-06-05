package com.adaptris.google.cloud.pubsub;

import java.util.LinkedHashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.pubsub.v1.Publisher;

public class PublisherMap extends LinkedHashMap<String, Publisher> {

  private static final long serialVersionUID = 2020060901L;

  protected transient Logger log = LoggerFactory.getLogger(this.getClass().getName());

  public static final int DEFAULT_MAX_ENTRIES = 10;

  private final int maxEntries;

  public PublisherMap() {
    this(DEFAULT_MAX_ENTRIES);
  }

  public PublisherMap(int maxEntries) {
    this.maxEntries = maxEntries;
  }

  @Override
  protected boolean removeEldestEntry(Map.Entry<String, Publisher> eldest) {
    if (size() > maxEntries) {
      try {
        if (eldest.getValue() != null) {
          eldest.getValue().shutdown();
        }
      } catch (Exception ignored) {
        log.trace("Publisher failed to shutdown", ignored);
      }
      return true;
    } else {
      return false;
    }
  }

  public int getMaxEntries() {
    return maxEntries;
  }

}
