package com.adaptris.google.cloud.pubsub;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.AdaptrisMessageFactory;

public class MetadataReplyProviderTest {

  @Test
  public void testConstruct() throws Exception {
    MetadataReplyProvider provider = new MetadataReplyProvider();
    assertEquals(ReplyProvider.AckReply.NACK, provider.getDefaultReply());
    assertNull(provider.getMetadataKey());
    provider = new MetadataReplyProvider("metadataKey");
    assertEquals(ReplyProvider.AckReply.NACK, provider.getDefaultReply());
    assertEquals("metadataKey", provider.getMetadataKey());
    provider = new MetadataReplyProvider("metadataKey", ReplyProvider.AckReply.ACK);
    assertEquals("metadataKey", provider.getMetadataKey());
    assertEquals(ReplyProvider.AckReply.ACK, provider.getDefaultReply());
  }

  @Test
  public void testGetAckReply() throws Exception {
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage();
    MetadataReplyProvider provider;
    provider = new MetadataReplyProvider(null, ReplyProvider.AckReply.ACK);
    assertEquals(ReplyProvider.AckReply.ACK, provider.getAckReply(msg));
    provider = new MetadataReplyProvider("key", ReplyProvider.AckReply.ACK);
    assertEquals(ReplyProvider.AckReply.ACK, provider.getAckReply(msg));
    msg.addMetadata("key", "SOMETHING_INVALID");
    assertEquals(ReplyProvider.AckReply.ACK, provider.getAckReply(msg));
    msg.addMetadata("key", "NACK");
    assertEquals(ReplyProvider.AckReply.NACK, provider.getAckReply(msg));
  }

  @Test
  public void testDefaultReply() throws Exception {
    MetadataReplyProvider provider = new MetadataReplyProvider();
    provider.setDefaultReply(ReplyProvider.AckReply.NACK);
    assertEquals(ReplyProvider.AckReply.NACK, provider.getDefaultReply());
    provider = new MetadataReplyProvider();
    provider.setDefaultReply(ReplyProvider.AckReply.ACK);
    assertEquals(ReplyProvider.AckReply.ACK, provider.getDefaultReply());
  }

  @Test
  public void testGetMetadataKey() throws Exception {
    MetadataReplyProvider provider = new MetadataReplyProvider();
    provider.setMetadataKey("key");
    assertEquals("key", provider.getMetadataKey());
  }

}