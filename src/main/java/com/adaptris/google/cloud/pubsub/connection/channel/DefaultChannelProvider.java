package com.adaptris.google.cloud.pubsub.connection.channel;

import com.google.api.gax.grpc.InstantiatingChannelProvider;
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings;
import com.thoughtworks.xstream.annotations.XStreamAlias;

@XStreamAlias("default-channel-provider")
public class DefaultChannelProvider extends ChannelProvider {

  @Override
  com.google.api.gax.grpc.ChannelProvider createChannelProvider() {
    return SubscriptionAdminSettings.defaultChannelProviderBuilder().build();
  }
}
