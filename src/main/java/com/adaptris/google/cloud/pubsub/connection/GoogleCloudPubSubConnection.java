package com.adaptris.google.cloud.pubsub.connection;


import com.adaptris.core.AdaptrisConnectionImp;
import com.adaptris.core.CoreException;
import com.adaptris.google.cloud.credential.CredentialBuilder;
import com.adaptris.google.cloud.credential.GoogleCredentialBuilder;
import com.adaptris.google.cloud.pubsub.consumer.GoogleCloudPubSubConfig;
import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.pubsub.v1.PushConfig;
import com.google.pubsub.v1.SubscriptionName;
import com.google.pubsub.v1.TopicName;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import com.thoughtworks.xstream.annotations.XStreamImplicit;
import org.apache.commons.lang.StringUtils;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.util.List;

@XStreamAlias("google-cloud-pubsub-connection")
public class GoogleCloudPubSubConnection extends AdaptrisConnectionImp {

  @NotNull
  @Valid
  private String projectName;

  @NotNull
  @Valid
  private String jsonKeyFile;

  @NotNull
  @Valid
  @XStreamImplicit(itemFieldName = "scope")
  private List<String> scopes;

  private transient CredentialBuilder credentialBuilder = new GoogleCredentialBuilder();

  private transient CredentialsProvider credentialsProvider;


  @Override
  protected void prepareConnection() throws CoreException {
    if (StringUtils.isEmpty(getProjectName())){
      throw new CoreException("Project Name is invalid");
    }
    if (StringUtils.isEmpty(getJsonKeyFile())){
      throw new CoreException("Json Key File is invalid");
    }
    if(getScopes() == null || getScopes().size() == 0){
      throw new CoreException("Scope is invalid");
    }
  }

  @Override
  protected void initConnection() throws CoreException {
    credentialsProvider = FixedCredentialsProvider.create(credentialBuilder.fromStreamWithScope(getJsonKeyFile(), getScopes()));
  }

  @Override
  protected void startConnection() throws CoreException {
  }

  @Override
  protected void stopConnection() {
  }

  @Override
  protected void closeConnection() {
  }

  public SubscriptionName createSubscription(GoogleCloudPubSubConfig config) throws CoreException {
    SubscriptionName subscription = SubscriptionName.create(getProjectName(), config.getSubscriptionName());
    if (config.getCreateSubscription()) {
      TopicName topic = TopicName.create(getProjectName(), config.getTopicName());
      try (SubscriptionAdminClient subscriptionAdminClient = SubscriptionAdminClient.create(SubscriptionAdminSettings.defaultBuilder().setCredentialsProvider(credentialsProvider).build())) {
        subscriptionAdminClient.createSubscription(subscription, topic, PushConfig.getDefaultInstance(), config.getAckDeadlineSeconds());
      } catch (Exception e) {
        throw new CoreException("Could not create subscription", e);
      }
    }
    return subscription;
  }

  public void deleteSubscription(GoogleCloudPubSubConfig config) throws CoreException {
    SubscriptionName subscription = SubscriptionName.create(getProjectName(), config.getSubscriptionName());
    if (config.getCreateSubscription()) {
      try (SubscriptionAdminClient subscriptionAdminClient = SubscriptionAdminClient.create(SubscriptionAdminSettings.defaultBuilder().setCredentialsProvider(credentialsProvider).build())) {
        subscriptionAdminClient.deleteSubscription(subscription);
      } catch (Exception e) {
        throw new CoreException("Could not delete subscription", e);
      }
    }
  }

  public Subscriber createSubscriber(SubscriptionName subscription, MessageReceiver receiver) {
    Subscriber subscriber = Subscriber.defaultBuilder(subscription, receiver).setCredentialsProvider(credentialsProvider).build();
    subscriber.addListener(
        new Subscriber.Listener() {
          @Override
          public void failed(Subscriber.State from, Throwable failure) {
            log.error("Subscriber encountered a fatal error and is shutting down", failure);
          }
        },
        MoreExecutors.directExecutor());
    return subscriber;
  }

  public String getProjectName() {
    return projectName;
  }

  public void setProjectName(String projectName) {
    this.projectName = projectName;
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

}
