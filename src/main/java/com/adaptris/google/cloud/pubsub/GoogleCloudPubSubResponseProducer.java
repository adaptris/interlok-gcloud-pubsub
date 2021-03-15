package com.adaptris.google.cloud.pubsub;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

import com.adaptris.annotation.AutoPopulated;
import com.adaptris.annotation.ComponentProfile;
import com.adaptris.annotation.DisplayOrder;
import com.adaptris.annotation.InputFieldDefault;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.CoreException;
import com.adaptris.core.NullConnection;
import com.adaptris.core.ProduceException;
import com.adaptris.core.ProduceOnlyProducerImp;
import com.adaptris.interlok.util.Args;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.thoughtworks.xstream.annotations.XStreamAlias;

import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

/**
 * @config google-cloud-pubsub-response-producer
 */
@XStreamAlias("google-cloud-pubsub-response-producer")
@ComponentProfile(summary = "Publish a ack/nack message to Google pubsub (when auto-acknowledge=false on the consumer)", tag = "producer,gcloud,messaging", recommended =
{
    NullConnection.class
})
@DisplayOrder(order =
{
    "replyProvider"
})
public class GoogleCloudPubSubResponseProducer extends ProduceOnlyProducerImp {

  @Valid
  @NotNull
  @AutoPopulated
  @InputFieldDefault("ConfiguredReplyProvider with a NACK")
  @NonNull
  @Getter
  @Setter
  private ReplyProvider replyProvider;

  public GoogleCloudPubSubResponseProducer(){
    setReplyProvider(new ConfiguredReplyProvider(ReplyProvider.AckReply.NACK));
  }

  public GoogleCloudPubSubResponseProducer(ReplyProvider.AckReply reply){
    setReplyProvider(new ConfiguredReplyProvider(reply));
  }

  @Override
  protected void doProduce(AdaptrisMessage msg, String ignored) throws ProduceException {
    AckReplyConsumer ackReplyConsumer =
        (AckReplyConsumer) msg.getObjectHeaders().get(Constants.REPLY_KEY);
    if (ackReplyConsumer == null){
      log.debug("No AckReplyConsumer in object metadata, nothing to do");
      return;
    }
    getReplyProvider().getAckReply(msg).execute(ackReplyConsumer);
  }

  @Override
  public void prepare() throws CoreException {
    Args.notNull(getReplyProvider(), "reply-provider");
  }

  @Override
  public String endpoint(AdaptrisMessage msg) throws ProduceException {
    return null;
  }
}
