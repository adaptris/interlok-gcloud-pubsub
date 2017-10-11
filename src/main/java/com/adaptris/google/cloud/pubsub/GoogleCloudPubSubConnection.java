package com.adaptris.google.cloud.pubsub;


import com.adaptris.core.CoreException;
import com.adaptris.google.cloud.pubsub.channel.ChannelProvider;
import com.adaptris.google.cloud.pubsub.credentials.CredentialsProvider;
import com.google.api.gax.batching.FlowControlSettings;
import com.google.api.gax.grpc.ApiException;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.pubsub.v1.PushConfig;
import com.google.pubsub.v1.Subscription;
import com.google.pubsub.v1.SubscriptionName;
import com.google.pubsub.v1.TopicName;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import io.grpc.Status;
import org.apache.commons.lang.StringUtils;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

@XStreamAlias("google-cloud-pubsub-connection")
public class GoogleCloudPubSubConnection extends ConnectionConfig {

  @NotNull
  @Valid
  private String projectName;


  public GoogleCloudPubSubConnection(){
    super();
  }

  public GoogleCloudPubSubConnection(ChannelProvider channelProvider){
    super(channelProvider);
  }

  public GoogleCloudPubSubConnection(ChannelProvider channelProvider, CredentialsProvider credentialsProvider){
    super(channelProvider, credentialsProvider);
  }

  @Override
  protected void prepareConnection() throws CoreException {
    if (StringUtils.isEmpty(getProjectName())){
      throw new CoreException("Project Name is invalid");
    }
  }

  public Subscription createSubscription(ConsumeConfig config) throws CoreException {
    SubscriptionName subscriptionName = SubscriptionName.create(getProjectName(), config.getSubscriptionName());
    TopicName topic = TopicName.create(getProjectName(), config.getTopicName());
    try {
      Subscription subscription = getSubscriptionAdminClient().getSubscription(subscriptionName);
      log.trace(String.format("Found existing subscription [%s] for Topic [%s]", config.getSubscriptionName(), subscription.getTopic()));
      if(!subscription.getTopic().equals(topic.toString())){
        throw new CoreException(String.format("Existing subscription topics do not match [%s] [%s]", subscription.getTopic(), topic.toString()));
      }
      return subscription;
    } catch (ApiException e){
      if (Status.Code.NOT_FOUND != e.getStatusCode()){
        throw new CoreException("Failed to retrieve Topic", e);
      } else {
        if (config.getCreateSubscription()){
          log.trace(String.format("Creating Subscription [%s] Topic [%s]", config.getSubscriptionName(), topic.toString()));
          return getSubscriptionAdminClient().createSubscription(subscriptionName, topic, PushConfig.getDefaultInstance(), config.getAckDeadlineSeconds());
        } else {
          throw new CoreException("Failed to retrieve Topic", e);
        }
      }
    }
  }

  public void deleteSubscription(ConsumeConfig config) throws CoreException {
    SubscriptionName subscription = SubscriptionName.create(getProjectName(), config.getSubscriptionName());
    if (config.getCreateSubscription()) {
      log.trace("Deleting Subscription [{}] for Project [{}] Topic [{}]", config.getSubscriptionName(), getProjectName(), config.getTopicName());
      getSubscriptionAdminClient().deleteSubscription(subscription);
    }
  }

  public Subscriber createSubscriber(Subscription subscription, MessageReceiver receiver) {
    Subscriber.Builder subscriberBuilder = Subscriber.defaultBuilder(subscription.getNameAsSubscriptionName(), receiver)
        .setChannelProvider(getGoogleChannelProvider())
        .setCredentialsProvider(getGoogleCredentialsProvider());
    getFlowControlProvider().apply(subscriberBuilder);
    Subscriber subscriber = subscriberBuilder.build();
    if (getConnectionErrorHandler() instanceof GoogleCloudPubSubConnectionErrorHandler) {
      subscriber.addListener(
          new Subscriber.Listener() {
            @Override
            public void failed(Subscriber.State from, Throwable failure) {
              log.error("Subscriber failed", failure);
              getConnectionErrorHandler().handleConnectionException();
            }
          },
          MoreExecutors.directExecutor());
    }
    return subscriber;
  }


  public String getProjectName() {
    return projectName;
  }

  public void setProjectName(String projectName) {
    this.projectName = projectName;
  }

}
