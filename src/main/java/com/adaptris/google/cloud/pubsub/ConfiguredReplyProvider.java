package com.adaptris.google.cloud.pubsub;

import com.adaptris.core.AdaptrisMessage;
import com.thoughtworks.xstream.annotations.XStreamAlias;

@XStreamAlias("configured-reply-provider")
public class ConfiguredReplyProvider implements ReplyProvider {

  private AckReply reply;

  public ConfiguredReplyProvider(){
    setReply(AckReply.NACK);
  }

  public ConfiguredReplyProvider(AckReply ackReply){
    setReply(ackReply);
  }

  @Override
  public AckReply getAckReply(AdaptrisMessage message) {
    return reply;
  }

  public AckReply getReply() {
    return reply;
  }

  public void setReply(AckReply reply) {
    this.reply = reply;
  }
}
