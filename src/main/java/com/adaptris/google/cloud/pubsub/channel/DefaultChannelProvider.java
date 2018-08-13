package com.adaptris.google.cloud.pubsub.channel;

import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings;
import com.thoughtworks.xstream.annotations.XStreamAlias;

@XStreamAlias("default-channel-provider")
public class DefaultChannelProvider extends ChannelProvider {

  @Override
  TransportChannelProvider createChannelProvider() {
    return SubscriptionAdminSettings.defaultGrpcTransportProviderBuilder()
        .setMaxInboundMessageSize(MAX_INBOUND_MESSAGE_SIZE)
        .build();
  }
}
