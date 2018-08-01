package com.adaptris.google.cloud.pubsub.adminclient;


import com.adaptris.core.ComponentLifecycle;
import com.adaptris.core.CoreException;
import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.rpc.TransportChannelProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AdminClientProvider implements ComponentLifecycle {

  private transient CredentialsProvider credentialsProvider;
  private transient TransportChannelProvider channelProvider;
  protected transient Logger log = LoggerFactory.getLogger(this.getClass().getName());

  @Override
  public void start() throws CoreException {

  }

  @Override
  public void stop() {

  }


  public TransportChannelProvider getChannelProvider() {
    return channelProvider;
  }

  public void setChannelProvider(TransportChannelProvider channelProvider) {
    this.channelProvider = channelProvider;
  }

  public CredentialsProvider getCredentialsProvider() {
    return credentialsProvider;
  }

  public void setCredentialsProvider(CredentialsProvider credentialsProvider) {
    this.credentialsProvider = credentialsProvider;
  }
}
