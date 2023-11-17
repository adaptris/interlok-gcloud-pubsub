package com.adaptris.google.cloud.pubsub.channel;

import com.adaptris.core.ComponentLifecycle;
import com.adaptris.core.CoreException;
import com.google.api.gax.rpc.TransportChannelProvider;

public abstract class ChannelProvider implements ComponentLifecycle {

  static final int MAX_INBOUND_MESSAGE_SIZE = 20 * 1024 * 1024;

  private transient TransportChannelProvider channelProvider;

  abstract TransportChannelProvider createChannelProvider() throws CoreException;

  void validateArguments() throws CoreException {
  }

  @Override
  public void init() throws CoreException {
    validateArguments();
    setChannelProvider(createChannelProvider());
  }

  @Override
  public void start() throws CoreException {
  }

  @Override
  public void stop() {
  }

  @Override
  public void close() {
  }

  void setChannelProvider(TransportChannelProvider channelProvider) {
    this.channelProvider = channelProvider;
  }

  public TransportChannelProvider getChannelProvider() {
    return channelProvider;
  }

}
