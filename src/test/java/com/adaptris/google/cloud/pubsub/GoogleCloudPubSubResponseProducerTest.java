package com.adaptris.google.cloud.pubsub;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import org.junit.Test;
import org.mockito.Mockito;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.AdaptrisMessageFactory;
import com.adaptris.core.ProducerCase;
import com.adaptris.core.StandaloneProducer;
import com.adaptris.core.util.LifecycleHelper;
import com.google.cloud.pubsub.v1.AckReplyConsumer;

public class GoogleCloudPubSubResponseProducerTest extends ProducerCase {

  @Override
  public boolean isAnnotatedForJunit4() {
    return true;
  }
  @Test
  public void testConstruct() throws Exception {
    GoogleCloudPubSubResponseProducer producer = new GoogleCloudPubSubResponseProducer();
    assertNotNull(producer.getReplyProvider());
    assertTrue(producer.getReplyProvider() instanceof ConfiguredReplyProvider);
    assertEquals(ReplyProvider.AckReply.NACK, ((ConfiguredReplyProvider)producer.getReplyProvider()).getReply());
    producer = new GoogleCloudPubSubResponseProducer(ReplyProvider.AckReply.ACK);
    assertNotNull(producer.getReplyProvider());
    assertTrue(producer.getReplyProvider() instanceof ConfiguredReplyProvider);
    assertEquals(ReplyProvider.AckReply.ACK, ((ConfiguredReplyProvider)producer.getReplyProvider()).getReply());
  }

  @Test
  public void testProduceNullMetadata() throws Exception {
    GoogleCloudPubSubResponseProducer producer = Mockito.spy(new GoogleCloudPubSubResponseProducer());
    AdaptrisMessage msg =  AdaptrisMessageFactory.getDefaultInstance().newMessage();
    producer.produce(msg);
    Mockito.verify(producer, Mockito.never()).getReplyProvider();
  }

  @Test
  public void testProduceNack() throws Exception {
    GoogleCloudPubSubResponseProducer producer = Mockito.spy(new GoogleCloudPubSubResponseProducer());
    producer.setReplyProvider(new ConfiguredReplyProvider(ReplyProvider.AckReply.NACK));
    AdaptrisMessage msg =  AdaptrisMessageFactory.getDefaultInstance().newMessage();
    AckReplyConsumer ackReplyConsumer = Mockito.mock(AckReplyConsumer.class);
    msg.addObjectHeader(Constants.REPLY_KEY,ackReplyConsumer);
    producer.produce(msg);
    Mockito.verify(producer, Mockito.times(1)).getReplyProvider();
    Mockito.verify(ackReplyConsumer, Mockito.times(1)).nack();
    Mockito.verify(ackReplyConsumer, Mockito.never()).ack();
  }

  @Test
  public void testProduceAck() throws Exception {
    GoogleCloudPubSubResponseProducer producer = Mockito.spy(new GoogleCloudPubSubResponseProducer());
    producer.setReplyProvider(new ConfiguredReplyProvider(ReplyProvider.AckReply.ACK));
    AdaptrisMessage msg =  AdaptrisMessageFactory.getDefaultInstance().newMessage();
    AckReplyConsumer ackReplyConsumer = Mockito.mock(AckReplyConsumer.class);
    msg.addObjectHeader(Constants.REPLY_KEY,ackReplyConsumer);
    producer.produce(msg);
    Mockito.verify(producer, Mockito.times(1)).getReplyProvider();
    Mockito.verify(ackReplyConsumer, Mockito.times(1)).ack();
    Mockito.verify(ackReplyConsumer, Mockito.never()).nack();
  }

  @Test
  public void testPrepare() throws Exception {
    GoogleCloudPubSubResponseProducer producer = new GoogleCloudPubSubResponseProducer();
    producer.setReplyProvider(new ConfiguredReplyProvider(ReplyProvider.AckReply.ACK));
    LifecycleHelper.prepare(producer);
  }

  @Test
  public void testLifecycle() throws Exception {
    GoogleCloudPubSubResponseProducer producer = Mockito.spy(new GoogleCloudPubSubResponseProducer());
    LifecycleHelper.initAndStart(producer);
    LifecycleHelper.stopAndClose(producer);
    Mockito.verify(producer, Mockito.times(1)).prepare();
    Mockito.verify(producer, Mockito.times(1)).init();
    Mockito.verify(producer, Mockito.times(1)).start();
    Mockito.verify(producer, Mockito.times(1)).stop();
    Mockito.verify(producer, Mockito.times(1)).close();
  }

  @Test
  public void testGetReplyProvider() throws Exception {
    GoogleCloudPubSubResponseProducer producer = new GoogleCloudPubSubResponseProducer();
    producer.setReplyProvider(new MetadataReplyProvider());
    assertTrue(producer.getReplyProvider() instanceof  MetadataReplyProvider);
  }

  @Override
  protected Object retrieveObjectForSampleConfig() {
    GoogleCloudPubSubResponseProducer producer = new GoogleCloudPubSubResponseProducer();
    return new StandaloneProducer(producer);
  }
}