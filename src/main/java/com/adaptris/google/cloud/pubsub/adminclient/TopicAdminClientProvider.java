package com.adaptris.google.cloud.pubsub.adminclient;

import com.adaptris.core.CoreException;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminSettings;

import java.io.IOException;

public class TopicAdminClientProvider extends AdminClientProvider {

  private transient TopicAdminClient topicAdminClient;

  @Override
  public void init() throws CoreException {
    if(getChannelProvider() == null){
      throw new CoreException("ChannelProvider can not be null");
    }
    if(getCredentialsProvider() == null){
      throw new CoreException("CredentialsProvider can not be null");
    }
    try {
      topicAdminClient = TopicAdminClient.create(TopicAdminSettings.defaultBuilder().setChannelProvider(getChannelProvider()).setCredentialsProvider(getCredentialsProvider()).build());
    } catch (IOException e) {
      throw new CoreException("Failed to create SubscriptionAdminClient", e);
    }
  }

  @Override
  public void close() {
    if(topicAdminClient != null) {
      try {
        topicAdminClient.close();
      } catch (Exception e) {
        log.trace("SubscriptionAdminClient failed to close");
      }
    }
  }

  public TopicAdminClient getTopicAdminClient(){
    return topicAdminClient;
  }
}
