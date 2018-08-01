package com.adaptris.google.cloud.pubsub.channel;

import com.adaptris.core.CoreException;
import com.google.api.gax.rpc.TransportChannelProvider;

/**
 * @author mwarman
 */
public class MockChannelProvider extends ChannelProvider {

  private transient TransportChannelProvider channelProvider;

  public MockChannelProvider(TransportChannelProvider channelProvider){
    this.channelProvider = channelProvider;
  }

  @Override
  TransportChannelProvider createChannelProvider() throws CoreException {
    return channelProvider;
  }
}
