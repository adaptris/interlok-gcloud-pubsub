package com.adaptris.google.cloud.pubsub.channel;

import com.adaptris.core.util.LifecycleHelper;
import com.google.api.gax.grpc.InstantiatingGrpcChannelProvider;
import com.google.api.gax.rpc.TransportChannelProvider;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertTrue;

public class DefaultChannelProviderTest {

  @Test
  public void testInit() throws Exception {
    DefaultChannelProvider provider = Mockito.spy(new DefaultChannelProvider());
    LifecycleHelper.initAndStart(provider);
    assertTrue(provider.getChannelProvider() instanceof InstantiatingGrpcChannelProvider);
    Mockito.verify(provider,Mockito.times(1)).init();
    Mockito.verify(provider,Mockito.times(1)).start();
    Mockito.verify(provider,Mockito.times(1)).validateArguments();
    Mockito.verify(provider,Mockito.times(1)).setChannelProvider(Mockito.any(InstantiatingGrpcChannelProvider.class));
  }

  @Test
  public void testStopClose() throws Exception{
    DefaultChannelProvider provider = Mockito.spy(new DefaultChannelProvider());
    LifecycleHelper.stopAndClose(provider);
    Mockito.verify(provider,Mockito.times(1)).stop();
    Mockito.verify(provider,Mockito.times(1)).close();
    Mockito.verify(provider,Mockito.never()).validateArguments();
    Mockito.verify(provider,Mockito.never()).setChannelProvider(Mockito.any(InstantiatingGrpcChannelProvider.class));
  }

  @Test
  public void testCreateCredentialsProvider() throws Exception {
    DefaultChannelProvider provider = new DefaultChannelProvider();
    TransportChannelProvider credentialsProvider = provider.createChannelProvider();
    assertTrue(credentialsProvider instanceof InstantiatingGrpcChannelProvider);
  }

}