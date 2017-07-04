package com.adaptris.google.cloud.pubsub.connection;

import com.adaptris.annotation.AdvancedConfig;
import com.adaptris.core.AdaptrisConnectionImp;
import com.adaptris.core.CoreException;
import com.adaptris.google.cloud.pubsub.connection.channel.ChannelProvider;
import com.adaptris.google.cloud.pubsub.connection.channel.DefaultChannelProvider;
import com.adaptris.google.cloud.pubsub.connection.credentials.CredentialsProvider;
import com.adaptris.google.cloud.pubsub.connection.credentials.NoCredentialsProvider;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.io.IOException;

public abstract class ConsumerConnectionConfig extends AdaptrisConnectionImp {

  enum ConnectionState {
    Initialising, Initialised, Starting, Started, Stopping(true), Stopped(true), Closing(true), Closed(true);

    final boolean stopOrClose;

    ConnectionState(){
      this(false);
    }

    ConnectionState(boolean stopOrClose){
      this.stopOrClose = stopOrClose;
    }

    public boolean isStopOrClose() {
      return stopOrClose;
    }
  }

  private ConnectionState connectionState;

  @NotNull
  @Valid
  private CredentialsProvider credentialsProvider;

  @AdvancedConfig
  @NotNull
  @Valid
  private ChannelProvider channelProvider;

  private transient SubscriptionAdminClient subscriptionAdminClient;

  public ConsumerConnectionConfig(){
    this(new DefaultChannelProvider());
    connectionState = ConnectionState.Closed;
  }

  public ConsumerConnectionConfig(ChannelProvider channelProvider){
    this(channelProvider, new NoCredentialsProvider());
  }

  public ConsumerConnectionConfig(ChannelProvider channelProvider, CredentialsProvider credentialsProvider){
    setCredentialsProvider(credentialsProvider);
    setChannelProvider(channelProvider);
  }

  @Override
  protected void initConnection() throws CoreException {
    connectionState = ConnectionState.Initialising;
    getCredentialsProvider().init();
    getChannelProvider().init();
    initSubscriptionAdminClient();
    connectionState = ConnectionState.Initialised;
  }

  @Override
  protected void startConnection() throws CoreException {
    connectionState = ConnectionState.Starting;
    getCredentialsProvider().start();
    getChannelProvider().start();
    connectionState = ConnectionState.Started;
  }

  @Override
  protected void stopConnection() {
    connectionState = ConnectionState.Stopping;
    getCredentialsProvider().stop();
    getChannelProvider().stop();
    connectionState = ConnectionState.Stopped;
  }

  @Override
  protected void closeConnection() {
    connectionState = ConnectionState.Closing;
    closeSubscriptionAdminClient();
    getCredentialsProvider().close();
    getChannelProvider().close();
    connectionState = ConnectionState.Closed;
  }


  public CredentialsProvider getCredentialsProvider() {
    return credentialsProvider;
  }

  public void setCredentialsProvider(CredentialsProvider credentialsProvider) {
    this.credentialsProvider = credentialsProvider;
  }

  public com.google.api.gax.core.CredentialsProvider getGoogleCredentialsProvider() {
    return getCredentialsProvider().getCredentialsProvider();
  }

  public ChannelProvider getChannelProvider() {
    return channelProvider;
  }

  public void setChannelProvider(ChannelProvider channelProvider) {
    this.channelProvider = channelProvider;
  }

  public com.google.api.gax.grpc.ChannelProvider getGoogleChannelProvider() {
    return getChannelProvider().getChannelProvider();
  }

  public ConnectionState getConnectionState() {
    return connectionState;
  }

  void initSubscriptionAdminClient() throws CoreException {
    try {
      subscriptionAdminClient = SubscriptionAdminClient.create(SubscriptionAdminSettings.defaultBuilder().setChannelProvider(getGoogleChannelProvider()).setCredentialsProvider(getGoogleCredentialsProvider()).build());
    } catch (IOException e){
      throw new CoreException("Failed to start SubscriptionAdminClient", e);
    }
  }

  void closeSubscriptionAdminClient() {
    if(subscriptionAdminClient != null){
      try {
        subscriptionAdminClient.close();
      } catch (Exception ignored) {
        ;
      }
    }
  }

  SubscriptionAdminClient getSubscriptionAdminClient() {
    return subscriptionAdminClient;
  }
}
