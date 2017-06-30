package com.adaptris.google.cloud.pubsub.connection;


import com.adaptris.core.CoreException;
import com.adaptris.google.cloud.pubsub.connection.channel.ChannelProvider;
import com.adaptris.google.cloud.pubsub.connection.credentials.CredentialsProvider;
import com.adaptris.google.cloud.pubsub.consumer.GoogleCloudPubSubConfig;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.pubsub.v1.PushConfig;
import com.google.pubsub.v1.SubscriptionName;
import com.google.pubsub.v1.TopicName;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import org.apache.commons.lang.StringUtils;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

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

  public SubscriptionName createSubscription(GoogleCloudPubSubConfig config) throws CoreException {
    SubscriptionName subscription = SubscriptionName.create(getProjectName(), config.getSubscriptionName());
    if (config.getCreateSubscription()) {
      log.trace("Creating Subscription [{}] for Project [{}] Topic [{}]", config.getSubscriptionName(), getProjectName(), config.getTopicName());
      TopicName topic = TopicName.create(getProjectName(), config.getTopicName());
      try (SubscriptionAdminClient subscriptionAdminClient = SubscriptionAdminClient.create(SubscriptionAdminSettings.defaultBuilder().setChannelProvider(getGoogleChannelProvider()).setCredentialsProvider(getGoogleCredentialsProvider()).build())) {
        subscriptionAdminClient.createSubscription(subscription, topic, PushConfig.getDefaultInstance(), config.getAckDeadlineSeconds());
      } catch (Exception e) {
        final String message = "Could not create subscription";
        log.error(message, e);
        throw new CoreException(message, e);
      }
    }
    return subscription;
  }

  public void deleteSubscription(GoogleCloudPubSubConfig config) throws CoreException {
    SubscriptionName subscription = SubscriptionName.create(getProjectName(), config.getSubscriptionName());
    if (config.getCreateSubscription()) {
      try (SubscriptionAdminClient subscriptionAdminClient = SubscriptionAdminClient.create(SubscriptionAdminSettings.defaultBuilder().setChannelProvider(getGoogleChannelProvider()).setCredentialsProvider(getGoogleCredentialsProvider()).build())) {
        subscriptionAdminClient.deleteSubscription(subscription);
      } catch (Exception e) {
        log.error("Could not delete subscription", e);
        throw new CoreException("Could not delete subscription", e);
      }
    }
  }

  public Subscriber createSubscriber(SubscriptionName subscription, MessageReceiver receiver) {
    Subscriber subscriber = Subscriber.defaultBuilder(subscription, receiver).setChannelProvider(getGoogleChannelProvider()).setCredentialsProvider(getGoogleCredentialsProvider()).build();
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
