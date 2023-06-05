package com.adaptris.google.cloud.pubsub;

import com.adaptris.core.AdaptrisMessage;
import com.google.cloud.pubsub.v1.AckReplyConsumer;

public interface ReplyProvider {

  public enum AckReply {
    ACK {
      @Override
      public void execute(AckReplyConsumer consumer) {
        consumer.ack();
      }
    },
    NACK {
      @Override
      public void execute(AckReplyConsumer consumer) {
        consumer.nack();
      }
    };

    public abstract void execute(AckReplyConsumer consumer);

    public static AckReply valueOf(String name, AckReply defaultValue) {
      try {
        return AckReply.valueOf(name);
      } catch (NullPointerException | IllegalArgumentException e) {
        return defaultValue;
      }
    }
  }

  public AckReply getAckReply(AdaptrisMessage message);

}
