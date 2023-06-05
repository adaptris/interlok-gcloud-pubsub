package com.adaptris.google.cloud.pubsub;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.AdaptrisMessageFactory;

public class ConfiguredReplyProviderTest {

  @Test
  public void testConstruct() throws Exception {
    ConfiguredReplyProvider provider = new ConfiguredReplyProvider();
    assertEquals(ReplyProvider.AckReply.NACK, provider.getReply());
    provider = new ConfiguredReplyProvider(ReplyProvider.AckReply.ACK);
    assertEquals(ReplyProvider.AckReply.ACK, provider.getReply());

  }

  @Test
  public void testGetAckReply() throws Exception {
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage();
    ConfiguredReplyProvider provider = new ConfiguredReplyProvider();
    provider.setReply(ReplyProvider.AckReply.NACK);
    assertEquals(ReplyProvider.AckReply.NACK, provider.getAckReply(msg));
    provider = new ConfiguredReplyProvider();
    provider.setReply(ReplyProvider.AckReply.ACK);
    assertEquals(ReplyProvider.AckReply.ACK, provider.getAckReply(msg));
  }

  @Test
  public void testGetReply() throws Exception {
    ConfiguredReplyProvider provider = new ConfiguredReplyProvider();
    provider.setReply(ReplyProvider.AckReply.NACK);
    assertEquals(ReplyProvider.AckReply.NACK, provider.getReply());
    provider = new ConfiguredReplyProvider();
    provider.setReply(ReplyProvider.AckReply.ACK);
    assertEquals(ReplyProvider.AckReply.ACK, provider.getReply());
  }

}