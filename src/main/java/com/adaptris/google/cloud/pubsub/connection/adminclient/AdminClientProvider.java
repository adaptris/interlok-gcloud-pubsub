package com.adaptris.google.cloud.pubsub.connection.adminclient;


import com.adaptris.core.ComponentLifecycle;
import com.adaptris.core.CoreException;
import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.grpc.ChannelProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AdminClientProvider implements ComponentLifecycle {

  private transient CredentialsProvider credentialsProvider;
  private transient ChannelProvider channelProvider;
  protected transient Logger log = LoggerFactory.getLogger(this.getClass().getName());

  @Override
  public void start() throws CoreException {

  }

  @Override
  public void stop() {

  }


  public ChannelProvider getChannelProvider() {
    return channelProvider;
  }

  public void setChannelProvider(ChannelProvider channelProvider) {
    this.channelProvider = channelProvider;
  }

  public CredentialsProvider getCredentialsProvider() {
    return credentialsProvider;
  }

  public void setCredentialsProvider(CredentialsProvider credentialsProvider) {
    this.credentialsProvider = credentialsProvider;
  }
}
