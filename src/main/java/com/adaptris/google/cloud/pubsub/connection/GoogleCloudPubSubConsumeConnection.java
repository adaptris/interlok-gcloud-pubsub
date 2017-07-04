package com.adaptris.google.cloud.pubsub.connection;


import com.adaptris.core.CoreException;
import com.adaptris.google.cloud.pubsub.connection.channel.ChannelProvider;
import com.adaptris.google.cloud.pubsub.connection.credentials.CredentialsProvider;
import com.adaptris.google.cloud.pubsub.consumer.GoogleCloudPubSubConfig;
import com.google.api.gax.grpc.ApiException;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings;
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
import java.io.IOException;

@XStreamAlias("google-cloud-pubsub-consume-connection")
public class GoogleCloudPubSubConsumeConnection extends ConsumerConnectionConfig {

  @NotNull
  @Valid
  private String projectName;


  public GoogleCloudPubSubConsumeConnection(){
    super();
  }

  public GoogleCloudPubSubConsumeConnection(ChannelProvider channelProvider){
    super(channelProvider);
  }

  public GoogleCloudPubSubConsumeConnection(ChannelProvider channelProvider, CredentialsProvider credentialsProvider){
    super(channelProvider, credentialsProvider);
  }

  @Override
  protected void prepareConnection() throws CoreException {
    if (StringUtils.isEmpty(getProjectName())){
      throw new CoreException("Project Name is invalid");
    }
  }

  public Subscription createSubscription(GoogleCloudPubSubConfig config) throws CoreException {
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
        throw e;
      } else {
        if (config.getCreateSubscription()){
          log.trace(String.format("Creating Subscription [%s] Topic [%s]", config.getSubscriptionName(), topic.toString()));
          return getSubscriptionAdminClient().createSubscription(subscriptionName, topic, PushConfig.getDefaultInstance(), config.getAckDeadlineSeconds());
        } else {
          throw e;
        }
      }
    }
  }

  public void deleteSubscription(GoogleCloudPubSubConfig config) throws CoreException {
    SubscriptionName subscription = SubscriptionName.create(getProjectName(), config.getSubscriptionName());
    if (config.getCreateSubscription()) {
      log.trace("Deleting Subscription [{}] for Project [{}] Topic [{}]", config.getSubscriptionName(), getProjectName(), config.getTopicName());
      getSubscriptionAdminClient().deleteSubscription(subscription);
    }
  }

  public Subscriber createSubscriber(Subscription subscription, MessageReceiver receiver) {
    Subscriber subscriber = Subscriber.defaultBuilder(subscription.getNameAsSubscriptionName(), receiver).setChannelProvider(getGoogleChannelProvider()).setCredentialsProvider(getGoogleCredentialsProvider()).build();
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
