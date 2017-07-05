package com.adaptris.google.cloud.pubsub;

import com.adaptris.annotation.AdvancedConfig;
import com.adaptris.core.AdaptrisConnectionImp;
import com.adaptris.core.CoreException;
import com.adaptris.google.cloud.pubsub.adminclient.SubscriptionAdminClientProvider;
import com.adaptris.google.cloud.pubsub.adminclient.TopicAdminClientProvider;
import com.adaptris.google.cloud.pubsub.channel.ChannelProvider;
import com.adaptris.google.cloud.pubsub.channel.DefaultChannelProvider;
import com.adaptris.google.cloud.pubsub.credentials.CredentialsProvider;
import com.adaptris.google.cloud.pubsub.credentials.NoCredentialsProvider;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminClient;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

abstract class ConnectionConfig extends AdaptrisConnectionImp {

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

  private transient ConnectionState connectionState;

  @NotNull
  @Valid
  private CredentialsProvider credentialsProvider;

  @AdvancedConfig
  @NotNull
  @Valid
  private ChannelProvider channelProvider;

  private transient SubscriptionAdminClientProvider subscriptionAdminClientProvider = new SubscriptionAdminClientProvider();
  private transient TopicAdminClientProvider topicAdminClientProvider = new TopicAdminClientProvider();

  public ConnectionConfig(){
    this(new DefaultChannelProvider());
    connectionState = ConnectionState.Closed;
  }

  public ConnectionConfig(ChannelProvider channelProvider){
    this(channelProvider, new NoCredentialsProvider());
  }

  public ConnectionConfig(ChannelProvider channelProvider, CredentialsProvider credentialsProvider){
    setCredentialsProvider(credentialsProvider);
    setChannelProvider(channelProvider);
  }

  @Override
  protected void initConnection() throws CoreException {
    connectionState = ConnectionState.Initialising;
    getCredentialsProvider().init();
    getChannelProvider().init();
    subscriptionAdminClientProvider.setChannelProvider(getGoogleChannelProvider());
    subscriptionAdminClientProvider.setCredentialsProvider(getGoogleCredentialsProvider());
    topicAdminClientProvider.setChannelProvider(getGoogleChannelProvider());
    topicAdminClientProvider.setCredentialsProvider(getGoogleCredentialsProvider());
    subscriptionAdminClientProvider.init();
    topicAdminClientProvider.init();
    connectionState = ConnectionState.Initialised;
  }

  @Override
  protected void startConnection() throws CoreException {
    connectionState = ConnectionState.Starting;
    getCredentialsProvider().start();
    getChannelProvider().start();
    subscriptionAdminClientProvider.start();
    topicAdminClientProvider.start();
    connectionState = ConnectionState.Started;
  }

  @Override
  protected void stopConnection() {
    connectionState = ConnectionState.Stopping;
    getCredentialsProvider().stop();
    getChannelProvider().stop();
    subscriptionAdminClientProvider.stop();
    topicAdminClientProvider.stop();
    connectionState = ConnectionState.Stopped;
  }

  @Override
  protected void closeConnection() {
    connectionState = ConnectionState.Closing;
    getCredentialsProvider().close();
    getChannelProvider().close();
    subscriptionAdminClientProvider.close();
    topicAdminClientProvider.close();
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

  void setSubscriptionAdminClientProvider(SubscriptionAdminClientProvider subscriptionAdminClientProvider) {
    this.subscriptionAdminClientProvider = subscriptionAdminClientProvider;
  }

  void setTopicAdminClientProvider(TopicAdminClientProvider topicAdminClientProvider) {
    this.topicAdminClientProvider = topicAdminClientProvider;
  }

  public SubscriptionAdminClient getSubscriptionAdminClient() {
    return subscriptionAdminClientProvider.getSubscriptionAdminClient();
  }

  public TopicAdminClient getTopicAdminClient(){
    return topicAdminClientProvider.getTopicAdminClient();
  }
}
