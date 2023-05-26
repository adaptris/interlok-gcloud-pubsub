package com.adaptris.google.cloud.pubsub.adminclient;

import java.io.IOException;

import com.adaptris.core.CoreException;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings;

public class SubscriptionAdminClientProvider extends AdminClientProvider {

  private transient SubscriptionAdminClient subscriptionAdminClient;

  @Override
  public void init() throws CoreException {
    if (getChannelProvider() == null) {
      throw new CoreException("ChannelProvider can not be null");
    }
    if (getCredentialsProvider() == null) {
      throw new CoreException("CredentialsProvider can not be null");
    }
    try {
      subscriptionAdminClient = SubscriptionAdminClient.create(SubscriptionAdminSettings.newBuilder()
          .setTransportChannelProvider(getChannelProvider()).setCredentialsProvider(getCredentialsProvider()).build());
    } catch (IOException e) {
      throw new CoreException("Failed to create SubscriptionAdminClient", e);
    }
  }

  @Override
  public void close() {
    if (subscriptionAdminClient != null) {
      try {
        subscriptionAdminClient.close();
      } catch (Exception e) {
        log.trace("SubscriptionAdminClient failed to close");
      }
    }
  }

  public SubscriptionAdminClient getSubscriptionAdminClient() {
    return subscriptionAdminClient;
  }

}
