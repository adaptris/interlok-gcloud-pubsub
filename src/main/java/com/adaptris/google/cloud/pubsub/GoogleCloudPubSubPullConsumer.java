package com.adaptris.google.cloud.pubsub;

import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.CoreException;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.Subscription;
import com.thoughtworks.xstream.annotations.XStreamAlias;

import java.util.Map;

import static com.adaptris.core.AdaptrisMessageFactory.defaultIfNull;

@XStreamAlias("google-cloud-pubsub-pull-consumer")
public class GoogleCloudPubSubPullConsumer extends ConsumeConfig implements MessageReceiver {

  private transient Subscriber subscriber = null;
  private transient String projectName;

  public GoogleCloudPubSubPullConsumer(){
    super();
  }

  @Override
  public void init() throws CoreException {
    GoogleCloudPubSubConnection connection = retrieveConnection(GoogleCloudPubSubConnection.class);
    Subscription subscription = connection.createSubscription(this);
    subscriber = connection.createSubscriber(subscription, this);
    projectName = connection.getProjectName();
  }

  @Override
  public void start() throws CoreException {
    subscriber.startAsync().awaitRunning();
  }

  @Override
  public void stop() {
    if (subscriber != null) {
      subscriber.stopAsync();
    }
  }

  @Override
  public void close() {
    GoogleCloudPubSubConnection connection = retrieveConnection(GoogleCloudPubSubConnection.class);
    try {
      connection.deleteSubscription(this);
    } catch (CoreException e) {
      log.error("Could not delete subscription", e);
    }
  }


  @Override
  public void receiveMessage(PubsubMessage pubsubMessage, AckReplyConsumer consumer) {
    AdaptrisMessage adaptrisMessage = defaultIfNull(getMessageFactory()).newMessage(pubsubMessage.getData().toByteArray());
    adaptrisMessage.addMetadata("gcloud_topic", getTopicName());
    adaptrisMessage.addMetadata("gcloud_projectName", getProjectName());
    adaptrisMessage.addMetadata("gcloud_subscriptionName", getSubscriptionName());
    adaptrisMessage.addMetadata("gcloud_messageId", pubsubMessage.getMessageId());
    if(pubsubMessage.hasPublishTime()) {
      adaptrisMessage.addMetadata("gcloud_publishTime", String.valueOf(pubsubMessage.getPublishTime().getSeconds()));
    }
    for (Map.Entry<String, String> e : pubsubMessage.getAttributesMap().entrySet()) {
      adaptrisMessage.addMetadata(e.getKey(), e.getValue());
    }
    if(getAutoAcknowledge()) {
      consumer.ack();
    } else {
      adaptrisMessage.addObjectHeader(Constants.REPLY_KEY, consumer);
    }
    String oldName = renameThread();
    retrieveAdaptrisMessageListener().onAdaptrisMessage(adaptrisMessage);
    Thread.currentThread().setName(oldName);
  }

  String getProjectName() {
    return projectName;
  }

  void setProjectName(String projectName) {
    this.projectName = projectName;
  }
}
