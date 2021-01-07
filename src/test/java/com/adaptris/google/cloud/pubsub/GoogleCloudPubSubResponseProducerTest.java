package com.adaptris.google.cloud.pubsub;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.junit.Test;

import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.AdaptrisMessageFactory;
import com.adaptris.core.StandaloneProducer;
import com.adaptris.core.util.LifecycleHelper;
import com.adaptris.interlok.junit.scaffolding.ExampleProducerCase;
import com.google.cloud.pubsub.v1.AckReplyConsumer;

public class GoogleCloudPubSubResponseProducerTest extends ExampleProducerCase {

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
    GoogleCloudPubSubResponseProducer producer = spy(new GoogleCloudPubSubResponseProducer());
    AdaptrisMessage msg =  AdaptrisMessageFactory.getDefaultInstance().newMessage();
    producer.produce(msg);
    verify(producer, never()).getReplyProvider();
  }

  @Test
  public void testProduceNack() throws Exception {
    GoogleCloudPubSubResponseProducer producer = spy(new GoogleCloudPubSubResponseProducer());
    producer.setReplyProvider(new ConfiguredReplyProvider(ReplyProvider.AckReply.NACK));
    AdaptrisMessage msg =  AdaptrisMessageFactory.getDefaultInstance().newMessage();
    AckReplyConsumer ackReplyConsumer = mock(AckReplyConsumer.class);
    msg.addObjectHeader(Constants.REPLY_KEY,ackReplyConsumer);
    producer.produce(msg);
    verify(producer, times(1)).getReplyProvider();
    verify(ackReplyConsumer, times(1)).nack();
    verify(ackReplyConsumer, never()).ack();
  }

  @Test
  public void testProduceAck() throws Exception {
    GoogleCloudPubSubResponseProducer producer = spy(new GoogleCloudPubSubResponseProducer());
    producer.setReplyProvider(new ConfiguredReplyProvider(ReplyProvider.AckReply.ACK));
    AdaptrisMessage msg =  AdaptrisMessageFactory.getDefaultInstance().newMessage();
    AckReplyConsumer ackReplyConsumer = mock(AckReplyConsumer.class);
    msg.addObjectHeader(Constants.REPLY_KEY,ackReplyConsumer);
    producer.produce(msg);
    verify(producer, times(1)).getReplyProvider();
    verify(ackReplyConsumer, times(1)).ack();
    verify(ackReplyConsumer, never()).nack();
  }

  @Test
  public void testPrepare() throws Exception {
    GoogleCloudPubSubResponseProducer producer = new GoogleCloudPubSubResponseProducer();
    producer.setReplyProvider(new ConfiguredReplyProvider(ReplyProvider.AckReply.ACK));
    LifecycleHelper.prepare(producer);
  }

  @Test
  public void testLifecycle() throws Exception {
    GoogleCloudPubSubResponseProducer producer = spy(new GoogleCloudPubSubResponseProducer());
    LifecycleHelper.initAndStart(producer);
    LifecycleHelper.stopAndClose(producer);
    verify(producer, times(1)).prepare();
    verify(producer, times(1)).init();
    verify(producer, times(1)).start();
    verify(producer, times(1)).stop();
    verify(producer, times(1)).close();
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