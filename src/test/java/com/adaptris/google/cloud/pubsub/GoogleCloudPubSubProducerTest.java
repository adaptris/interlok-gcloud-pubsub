package com.adaptris.google.cloud.pubsub;

import com.adaptris.core.*;
import com.adaptris.core.metadata.NoOpMetadataFilter;
import com.adaptris.core.metadata.RemoveAllMetadataFilter;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.pubsub.v1.PubsubMessage;
import org.junit.Test;
import org.mockito.Mockito;

public class GoogleCloudPubSubProducerTest extends ProducerCase {

  public GoogleCloudPubSubProducerTest(String name) {
    super(name);
  }

  @Test
  public void testConstruct() throws Exception {
    GoogleCloudPubSubProducer producer = new GoogleCloudPubSubProducer();
    assertEquals(Boolean.FALSE, producer.getCreateTopic());
    assertTrue(producer.getMetadataFilter() instanceof NoOpMetadataFilter);
    assertEquals((Integer)PublisherMap.DEFAULT_MAX_ENTRIES, producer.getPublisherCacheLimit());
  }

  @Test
  public void testPrepare() throws Exception {
    GoogleCloudPubSubProducer producer = new GoogleCloudPubSubProducer();
    producer.prepare();
    producer.setCreateTopic(null);
    prepareFail(producer, "create-topic is invalid");
    producer.setCreateTopic(true);
    producer.setMetadataFilter(null);
    prepareFail(producer, "metadata-filter is invalid");
    producer.setMetadataFilter(new NoOpMetadataFilter());
    producer.setPublisherCacheLimit(null);
    prepareFail(producer, "publisher-cache-limit is invalid");
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
    Mockito.doThrow(new Exception()).when(publisher).shutdown();
    publisherMap.put("key1",publisher);
    publisherMap.put("key2",null);
    GoogleCloudPubSubProducer producer = new GoogleCloudPubSubProducer();
    producer.setPublisherCache(publisherMap);
    producer.close();
    Mockito.verify(publisher, Mockito.times(1)).shutdown();

  }

  @Test
  public void testCreateTopic() throws Exception {
    GoogleCloudPubSubProducer producer = new GoogleCloudPubSubProducer();
    assertEquals(Boolean.FALSE, producer.getCreateTopic());
    producer.setCreateTopic(true);
    assertEquals(Boolean.TRUE, producer.getCreateTopic());
  }

  @Test
  public void testMetadataFilter() throws Exception {
    GoogleCloudPubSubProducer producer = new GoogleCloudPubSubProducer();
    assertTrue(producer.getMetadataFilter() instanceof NoOpMetadataFilter);
    producer.setMetadataFilter(new RemoveAllMetadataFilter());
    assertTrue(producer.getMetadataFilter() instanceof RemoveAllMetadataFilter);
  }

  @Test
  public void testCacheLimit() throws Exception {
    GoogleCloudPubSubProducer producer = new GoogleCloudPubSubProducer();
    assertEquals((Integer)PublisherMap.DEFAULT_MAX_ENTRIES, producer.getPublisherCacheLimit());
    producer.setPublisherCacheLimit(5);
    assertEquals((Integer)5, producer.getPublisherCacheLimit());
  }

  private void prepareFail(GoogleCloudPubSubProducer producer, String message){
    try {
      producer.prepare();
      fail();
    } catch (CoreException expected){
      assertEquals(message, expected.getMessage());
    }
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

    GoogleCloudPubSubProducer producer = new GoogleCloudPubSubProducer();
    producer.setDestination(new ConfiguredProduceDestination("topic-name"));

    StandaloneRequestor result = new StandaloneRequestor();
    result.setConnection(conn);
    result.setProducer(producer);
    return result;
  }
}