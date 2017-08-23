package com.adaptris.google.cloud.pubsub.channel;

import com.adaptris.core.ComponentLifecycle;
import com.adaptris.core.CoreException;

public abstract class ChannelProvider implements ComponentLifecycle {

  static final int MAX_INBOUND_MESSAGE_SIZE = 20 * 1024 * 1024;

  private transient com.google.api.gax.grpc.ChannelProvider channelProvider;

  abstract com.google.api.gax.grpc.ChannelProvider createChannelProvider() throws CoreException;

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

  void setChannelProvider(com.google.api.gax.grpc.ChannelProvider channelProvider) {
    this.channelProvider = channelProvider;
  }

  public com.google.api.gax.grpc.ChannelProvider getChannelProvider() {
    return channelProvider;
  }
}
