package com.adaptris.google.cloud.pubsub;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import org.junit.Test;
import org.mockito.Mockito;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.AdaptrisMessageFactory;
import com.adaptris.core.ProducerCase;
import com.adaptris.core.StandaloneRequestor;
import com.adaptris.core.util.LifecycleHelper;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.pubsub.v1.PubsubMessage;

public class GoogleCloudPubSubProducerTest extends ProducerCase {

  @Override
  public boolean isAnnotatedForJunit4() {
    return true;
  }

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
    Publisher publisher = Mockito.mock(Publisher.class);
    publisherMap.put("key1",publisher);
    publisherMap.put("key2",null);

    GoogleCloudPubSubProducer producer = new GoogleCloudPubSubProducer();
    producer.setPublisherCache(publisherMap);
    producer.close();

    Mockito.verify(publisher, Mockito.times(1)).shutdown();

  }

  @Test
  public void testCloseException() throws Exception {
    PublisherMap publisherMap = new PublisherMap(5);
    Publisher publisher = Mockito.mock(Publisher.class);
    Mockito.doThrow(new IllegalArgumentException()).when(publisher).shutdown();
    publisherMap.put("key1",publisher);
    publisherMap.put("key2",null);
    GoogleCloudPubSubProducer producer = new GoogleCloudPubSubProducer();
    producer.setPublisherCache(publisherMap);
    producer.close();
    Mockito.verify(publisher, Mockito.times(1)).shutdown();

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