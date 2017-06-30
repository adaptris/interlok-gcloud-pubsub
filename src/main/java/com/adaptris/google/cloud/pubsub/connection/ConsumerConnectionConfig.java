package com.adaptris.google.cloud.pubsub.connection;

import com.adaptris.annotation.AdvancedConfig;
import com.adaptris.core.AdaptrisConnectionImp;
import com.adaptris.core.CoreException;
import com.adaptris.google.cloud.pubsub.connection.channel.ChannelProvider;
import com.adaptris.google.cloud.pubsub.connection.channel.CustomChannelProvider;
import com.adaptris.google.cloud.pubsub.connection.channel.DefaultChannelProvider;
import com.adaptris.google.cloud.pubsub.connection.credentials.CredentialsProvider;
import com.adaptris.google.cloud.pubsub.connection.credentials.NoCredentialsProvider;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

public abstract class ConsumerConnectionConfig extends AdaptrisConnectionImp {

  @NotNull
  @Valid
  private CredentialsProvider credentialsProvider;

  @AdvancedConfig
  @NotNull
  @Valid
  private ChannelProvider channelProvider;

  public ConsumerConnectionConfig(){
    this(new DefaultChannelProvider());
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
    getCredentialsProvider().init();
    getChannelProvider().init();
  }

  @Override
  protected void startConnection() throws CoreException {
    getCredentialsProvider().start();
    getChannelProvider().start();
  }

  @Override
  protected void stopConnection() {
    getCredentialsProvider().stop();
    getChannelProvider().stop();
  }

  @Override
  protected void closeConnection() {
    getCredentialsProvider().close();
    getChannelProvider().close();
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
}
