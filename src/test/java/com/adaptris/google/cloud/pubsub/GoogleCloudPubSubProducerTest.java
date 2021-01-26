package com.adaptris.google.cloud.pubsub;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.junit.Test;

import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.AdaptrisMessageFactory;
import com.adaptris.core.StandaloneRequestor;
import com.adaptris.core.util.LifecycleHelper;
import com.adaptris.interlok.junit.scaffolding.ExampleProducerCase;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.pubsub.v1.PubsubMessage;

public class GoogleCloudPubSubProducerTest extends ExampleProducerCase {

  @Test
  public void testPrepare() throws Exception {
    GoogleCloudPubSubProducer producer = new GoogleCloudPubSubProducer();
    try {
      LifecycleHelper.prepare(producer);
      fail();
    } catch (Exception expected) {

    }
    producer.setTopic("topic");
    LifecycleHelper.prepare(producer);
  }

  @Test
  public void testClose() throws Exception {
    PublisherMap publisherMap = new PublisherMap(5);
    Publisher publisher = mock(Publisher.class);
    publisherMap.put("key1",publisher);
    publisherMap.put("key2",null);

    GoogleCloudPubSubProducer producer = new GoogleCloudPubSubProducer();
    producer.setPublisherCache(publisherMap);
    producer.close();

    verify(publisher, times(1)).shutdown();

  }

  @Test
  public void testCloseException() throws Exception {
    PublisherMap publisherMap = new PublisherMap(5);
    Publisher publisher = mock(Publisher.class);
    doThrow(new IllegalArgumentException()).when(publisher).shutdown();
    publisherMap.put("key1",publisher);
    publisherMap.put("key2",null);
    GoogleCloudPubSubProducer producer = new GoogleCloudPubSubProducer();
    producer.setPublisherCache(publisherMap);
    producer.close();
    verify(publisher, times(1)).shutdown();

  }


  @Test
  public void testCreatePubsubMessage() throws Exception{
    GoogleCloudPubSubProducer producer = new GoogleCloudPubSubProducer();
    final AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage("Hello World");
    msg.addMetadata("foo", "bar");
    PubsubMessage pusubMessage = producer.createPubsubMessage(msg);
    assertEquals("Hello World", new String(pusubMessage.getData().toByteArray()));
    assertTrue(pusubMessage.getAttributesMap().containsKey("foo"));
    assertEquals("bar", pusubMessage.getAttributesMap().get("foo"));
  }

  @Override
  protected Object retrieveObjectForSampleConfig() {
    GoogleCloudPubSubConnection conn = new GoogleCloudPubSubConnection();
    conn.setProjectName("project-name");

    GoogleCloudPubSubProducer producer = new GoogleCloudPubSubProducer().withTopic("topic-name");
    return new StandaloneRequestor(conn, producer);
  }
}