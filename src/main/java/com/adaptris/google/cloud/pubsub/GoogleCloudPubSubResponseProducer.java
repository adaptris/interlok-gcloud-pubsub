package com.adaptris.google.cloud.pubsub;

import com.adaptris.core.*;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.thoughtworks.xstream.annotations.XStreamAlias;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

@XStreamAlias("google-cloud-pubsub-response-producer")
public class GoogleCloudPubSubResponseProducer extends ProduceOnlyProducerImp {

  @Valid
  @NotNull
  private ReplyProvider replyProvider;

  public GoogleCloudPubSubResponseProducer(){
    setReplyProvider(new ConfiguredReplyProvider(ReplyProvider.AckReply.NACK));
  }

  public GoogleCloudPubSubResponseProducer(ReplyProvider.AckReply reply){
    setReplyProvider(new ConfiguredReplyProvider(reply));
  }

  @Override
  public void produce(AdaptrisMessage msg, ProduceDestination destination) throws ProduceException {
    AckReplyConsumer ackReplyConsumer = (AckReplyConsumer)msg.getObjectHeaders().get(Constants.REPLY_KEY);
    if (ackReplyConsumer == null){
      log.debug("No AckReplyConsumer in object metadata, nothing to do");
      return;
    }
    getReplyProvider().getAckReply(msg).execute(ackReplyConsumer);
  }

  @Override
  public void prepare() throws CoreException {
    if(getReplyProvider() == null){
      throw new CoreException("reply-provider is invalid");
    }
  }

  @Override
  public void init() throws CoreException {

  }

  @Override
  public void start() throws CoreException {

  }

  @Override
  public void stop() {

  }

  @Override
  public void close() {

  }

  public void setReplyProvider(ReplyProvider replyProvider) {
    this.replyProvider = replyProvider;
  }

  public ReplyProvider getReplyProvider() {
    return replyProvider;
  }
}
