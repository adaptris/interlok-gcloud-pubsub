package com.adaptris.google.cloud.pubsub;

import com.google.cloud.pubsub.v1.Publisher;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Map;

import static org.junit.Assert.*;

public class PublisherMapTest {

  @Test
  public void testMaxEntries() throws Exception {
    PublisherMap publisherMap = new PublisherMap();
    assertEquals(PublisherMap.DEFAULT_MAX_ENTRIES, publisherMap.getMaxEntries());
    publisherMap  = new PublisherMap(5);
    assertEquals(5, publisherMap.getMaxEntries());
  }

  @Test
  public void testRemoveEldestEntry() throws Exception {
    PublisherMap publisherMap = new PublisherMap(5);
    Publisher publisher = Mockito.mock(Publisher.class);
    publisherMap.put("key1",publisher);
    publisherMap.put("key2",null);
    publisherMap.put("key3",Mockito.mock(Publisher.class));
    publisherMap.put("key4",Mockito.mock(Publisher.class));
    publisherMap.put("key5",Mockito.mock(Publisher.class));

    assertTrue(publisherMap.containsKey("key1"));
    assertTrue(publisherMap.containsKey("key2"));
    assertTrue(publisherMap.containsKey("key3"));
    assertTrue(publisherMap.containsKey("key4"));
    assertTrue(publisherMap.containsKey("key5"));

    publisherMap.put("key6",Mockito.mock(Publisher.class));

    assertFalse(publisherMap.containsKey("key1"));
    assertTrue(publisherMap.containsKey("key2"));
    assertTrue(publisherMap.containsKey("key3"));
    assertTrue(publisherMap.containsKey("key4"));
    assertTrue(publisherMap.containsKey("key5"));
    assertTrue(publisherMap.containsKey("key6"));

    publisherMap.put("key7",Mockito.mock(Publisher.class));

    assertFalse(publisherMap.containsKey("key1"));
    assertFalse(publisherMap.containsKey("key2"));
    assertTrue(publisherMap.containsKey("key3"));
    assertTrue(publisherMap.containsKey("key4"));
    assertTrue(publisherMap.containsKey("key5"));
    assertTrue(publisherMap.containsKey("key6"));
    assertTrue(publisherMap.containsKey("key7"));

    Mockito.verify(publisher, Mockito.times(1)).shutdown();

  }

  @Test
  public void testRemoveEldestEntryException() throws Exception {
    PublisherMap publisherMap = new PublisherMap(5);
    Publisher publisher = Mockito.mock(Publisher.class);
    Mockito.doThrow(new Exception()).when(publisher).shutdown();
    publisherMap.put("key1",publisher);
    publisherMap.put("key2",null);
    publisherMap.put("key3",Mockito.mock(Publisher.class));
    publisherMap.put("key4",Mockito.mock(Publisher.class));
    publisherMap.put("key5",Mockito.mock(Publisher.class));

    assertTrue(publisherMap.containsKey("key1"));
    assertTrue(publisherMap.containsKey("key2"));
    assertTrue(publisherMap.containsKey("key3"));
    assertTrue(publisherMap.containsKey("key4"));
    assertTrue(publisherMap.containsKey("key5"));

    publisherMap.put("key6",Mockito.mock(Publisher.class));

    assertFalse(publisherMap.containsKey("key1"));
    assertTrue(publisherMap.containsKey("key2"));
    assertTrue(publisherMap.containsKey("key3"));
    assertTrue(publisherMap.containsKey("key4"));
    assertTrue(publisherMap.containsKey("key5"));
    assertTrue(publisherMap.containsKey("key6"));
    Mockito.verify(publisher, Mockito.times(1)).shutdown();
  }


}