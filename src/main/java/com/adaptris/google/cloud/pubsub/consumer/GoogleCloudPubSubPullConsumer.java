package com.adaptris.google.cloud.pubsub.consumer;

import com.adaptris.core.*;
import com.adaptris.google.cloud.pubsub.connection.GoogleCloudPubSubConsumeConnection;
import com.google.cloud.pubsub.v1.*;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.SubscriptionName;
import com.thoughtworks.xstream.annotations.XStreamAlias;

import java.util.Map;

import static com.adaptris.core.AdaptrisMessageFactory.defaultIfNull;

@XStreamAlias("google-cloud-pubsub-pull-consumer")
public class GoogleCloudPubSubPullConsumer extends GoogleCloudPubSubConfig implements MessageReceiver {

  private transient Subscriber subscriber = null;
  private transient String projectName;

  public GoogleCloudPubSubPullConsumer(){
    super();
  }

  @Override
  public void init() throws CoreException {
    GoogleCloudPubSubConsumeConnection connection = retrieveConnection(GoogleCloudPubSubConsumeConnection.class);
    SubscriptionName subscriptionName = connection.createSubscription(this);
    subscriber = connection.createSubscriber(subscriptionName, this);
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
    GoogleCloudPubSubConsumeConnection connection = retrieveConnection(GoogleCloudPubSubConsumeConnection.class);
    try {
      connection.deleteSubscription(this);
    } catch (CoreException e) {
      log.error("Could not delete subscription", e);
    }
  }


  @Override
  public void receiveMessage(PubsubMessage pubsubMessage, AckReplyConsumer consumer) {
    log.trace("PubsubMessage Received [%s]", pubsubMessage.getMessageId());
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
    retrieveAdaptrisMessageListener().onAdaptrisMessage(adaptrisMessage);
    consumer.ack();
  }

  String getProjectName() {
    return projectName;
  }

  void setProjectName(String projectName) {
    this.projectName = projectName;
  }
}
