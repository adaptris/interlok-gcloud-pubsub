package com.adaptris.google.cloud.pubsub.consumer;

import com.adaptris.core.*;
import com.adaptris.google.cloud.credential.CredentialProvider;
import com.adaptris.google.cloud.credential.GoogleCredentialProvider;
import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.cloud.pubsub.v1.*;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PushConfig;
import com.google.pubsub.v1.SubscriptionName;
import com.google.pubsub.v1.TopicName;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import com.thoughtworks.xstream.annotations.XStreamImplicit;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import static com.adaptris.core.AdaptrisMessageFactory.defaultIfNull;

@XStreamAlias("google-cloud-pubsub-consumer")
public class GoogleCloudPubSubPullConsumer extends AdaptrisMessageConsumerImp {

  @NotNull
  @Valid
  private String projectName;

  @NotNull
  @Valid
  private String subscriptionName;

  @NotNull
  @Valid
  private String jsonKeyFile;

  @NotNull
  @Valid
  @XStreamImplicit(itemFieldName = "scope")
  private List<String> scopes;

  private transient Subscriber subscriber = null;
  private transient CredentialProvider credentialProvider = new GoogleCredentialProvider();

  public GoogleCloudPubSubPullConsumer(){
  }

  @Override
  public void prepare() throws CoreException {
    if(getProjectName() == null){
      throw new CoreException("Project Name is invalid");
    }
    if(getSubscriptionName() == null){
      throw new CoreException("Subscription Name is invalid");
    }
    if(getJsonKeyFile() == null){
      throw new CoreException("Json Key File is invalid");
    }
    if(getScopes() == null){
      throw new CoreException("Scopes is invalid");
    }
    if(getDestination() == null){
      throw new CoreException("Destination is invalid");
    }
  }

  @Override
  public void init() throws CoreException {

    MessageReceiver receiver =
        new MessageReceiver() {
          @Override
          public void receiveMessage(PubsubMessage pubsubMessage, AckReplyConsumer consumer) {
            log.trace("PubsubMessage Received [%s]", pubsubMessage.getMessageId());
            AdaptrisMessage adaptrisMessage = defaultIfNull(getMessageFactory()).newMessage(pubsubMessage.getData().toByteArray());
            if(pubsubMessage.getMessageId() != null) {
              adaptrisMessage.addMetadata("gcloud_messageId", pubsubMessage.getMessageId());
            }
            if(pubsubMessage.hasPublishTime()) {
              adaptrisMessage.addMetadata("gcloud_publishTime", String.valueOf(pubsubMessage.getPublishTime().getSeconds()));
            }
            for (Map.Entry<String, String> e : pubsubMessage.getAttributesMap().entrySet()) {
              adaptrisMessage.addMetadata(e.getKey(), e.getValue());
            }
            retrieveAdaptrisMessageListener().onAdaptrisMessage(adaptrisMessage);
            consumer.ack();
          }
        };
    CredentialsProvider credentialsProvider = FixedCredentialsProvider.create(
        credentialProvider.fromStreamWithScope(getJsonKeyFile(), getScopes()));
    SubscriptionName subscription = SubscriptionName.create(getProjectName(), getSubscriptionName());
    TopicName topic = TopicName.create(getProjectName(), getDestination().getDestination());
    try (SubscriptionAdminClient subscriptionAdminClient = SubscriptionAdminClient.create(SubscriptionAdminSettings.defaultBuilder().setCredentialsProvider(credentialsProvider).build())) {
      subscriptionAdminClient.createSubscription(subscription, topic, PushConfig.getDefaultInstance(), 5);
    } catch (Exception e) {
      throw new CoreException("Could not c");
    }

    subscriber = Subscriber.defaultBuilder(subscription, receiver).setCredentialsProvider(credentialsProvider).build();
    subscriber.addListener(
        new Subscriber.Listener() {
          @Override
          public void failed(Subscriber.State from, Throwable failure) {
            log.error("Subscriber encountered a fatal error and is shutting down", failure);
          }
        },
        MoreExecutors.directExecutor());
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

  }

  public String getProjectName() {
    return projectName;
  }

  public void setProjectName(String projectName) {
    this.projectName = projectName;
  }

  public String getSubscriptionName() {
    return subscriptionName;
  }

  public void setSubscriptionName(String subscriptionName) {
    this.subscriptionName = subscriptionName;
  }

  public String getJsonKeyFile() {
    return jsonKeyFile;
  }

  public void setJsonKeyFile(String jsonKeyFile) {
    this.jsonKeyFile = jsonKeyFile;
  }

  public List<String> getScopes() {
    return scopes;
  }

  public void setScopes(List<String> scopes) {
    this.scopes = scopes;
  }

  CredentialProvider getCredentialProvider() {
    return credentialProvider;
  }

  void setCredentialProvider(CredentialProvider credentialProvider) {
    this.credentialProvider = credentialProvider;
  }
}
