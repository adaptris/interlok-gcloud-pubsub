package com.adaptris.google.cloud.pubsub.connection.adminclient;

import com.adaptris.core.CoreException;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings;

import java.io.IOException;

public class SubscriptionAdminClientProvider extends AdminClientProvider {

  private transient SubscriptionAdminClient subscriptionAdminClient;

  @Override
  public void init() throws CoreException {
    if(getChannelProvider() == null){
      throw new CoreException("ChannelProvider can not be null");
    }
    if(getCredentialsProvider() == null){
      throw new CoreException("CredentialsProvider can not be null");
    }
    try {
      subscriptionAdminClient = SubscriptionAdminClient.create(SubscriptionAdminSettings.defaultBuilder().setChannelProvider(getChannelProvider()).setCredentialsProvider(getCredentialsProvider()).build());
    } catch (IOException e) {
      throw new CoreException("Failed to create SubscriptionAdminClient", e);
    }
  }

  @Override
  public void close() {
    if(subscriptionAdminClient != null) {
      try {
        subscriptionAdminClient.close();
      } catch (Exception e) {
        log.trace("SubscriptionAdminClient failed to close");
      }
    }
  }

  public SubscriptionAdminClient getSubscriptionAdminClient(){
    return subscriptionAdminClient;
  }
}
