package com.adaptris.google.cloud.pubsub.channel;

import com.google.cloud.pubsub.v1.SubscriptionAdminSettings;
import com.thoughtworks.xstream.annotations.XStreamAlias;

@XStreamAlias("default-channel-provider")
public class DefaultChannelProvider extends ChannelProvider {

  @Override
  com.google.api.gax.grpc.ChannelProvider createChannelProvider() {
    return SubscriptionAdminSettings.defaultChannelProviderBuilder()
        .setMaxInboundMessageSize(MAX_INBOUND_MESSAGE_SIZE)
        .build();
  }
}
