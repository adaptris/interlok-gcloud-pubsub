package com.adaptris.google.cloud.pubsub.channel;

import com.adaptris.core.CoreException;

/**
 * @author mwarman
 */
public class MockChannelProvider extends ChannelProvider {

  private transient com.google.api.gax.grpc.ChannelProvider channelProvider;

  public MockChannelProvider(com.google.api.gax.grpc.ChannelProvider channelProvider){
    this.channelProvider = channelProvider;
  }

  @Override
  com.google.api.gax.grpc.ChannelProvider createChannelProvider() throws CoreException {
    return channelProvider;
  }
}
