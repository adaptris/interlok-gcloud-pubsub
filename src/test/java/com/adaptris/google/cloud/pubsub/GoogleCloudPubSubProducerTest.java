package com.adaptris.google.cloud.pubsub;

import com.adaptris.core.ConfiguredProduceDestination;
import com.adaptris.core.CoreException;
import com.adaptris.core.ProducerCase;
import com.adaptris.core.StandaloneRequestor;
import com.adaptris.core.metadata.NoOpMetadataFilter;
import com.adaptris.core.metadata.RemoveAllMetadataFilter;
import org.junit.Test;

public class GoogleCloudPubSubProducerTest extends ProducerCase {

  public GoogleCloudPubSubProducerTest(String name) {
    super(name);
  }

  @Test
  public void testConstruct() throws Exception {
    GoogleCloudPubSubProducer producer = new GoogleCloudPubSubProducer();
    assertEquals(Boolean.TRUE, producer.getCreateTopic());
    assertTrue(producer.getMetadataFilter() instanceof NoOpMetadataFilter);
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
  }

  @Test
  public void testCreateTopic() throws Exception {
    GoogleCloudPubSubProducer producer = new GoogleCloudPubSubProducer();
    assertEquals(Boolean.TRUE, producer.getCreateTopic());
    producer.setCreateTopic(false);
    assertEquals(Boolean.FALSE, producer.getCreateTopic());
  }

  @Test
  public void testMetadataFilter() throws Exception {
    GoogleCloudPubSubProducer producer = new GoogleCloudPubSubProducer();
    assertTrue(producer.getMetadataFilter() instanceof NoOpMetadataFilter);
    producer.setMetadataFilter(new RemoveAllMetadataFilter());
    assertTrue(producer.getMetadataFilter() instanceof RemoveAllMetadataFilter);
  }

  private void prepareFail(GoogleCloudPubSubProducer producer, String message){
    try {
      producer.prepare();
      fail();
    } catch (CoreException expected){
      assertEquals(message, expected.getMessage());
    }
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