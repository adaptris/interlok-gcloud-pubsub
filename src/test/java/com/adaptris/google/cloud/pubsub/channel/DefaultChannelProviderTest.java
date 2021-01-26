package com.adaptris.google.cloud.pubsub.channel;

import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.junit.Test;

import com.adaptris.core.util.LifecycleHelper;
import com.google.api.gax.grpc.InstantiatingGrpcChannelProvider;
import com.google.api.gax.rpc.TransportChannelProvider;

public class DefaultChannelProviderTest {

  @Test
  public void testInit() throws Exception {
    DefaultChannelProvider provider = spy(new DefaultChannelProvider());
    LifecycleHelper.initAndStart(provider);
    assertTrue(provider.getChannelProvider() instanceof InstantiatingGrpcChannelProvider);
    verify(provider, times(1)).init();
    verify(provider, times(1)).start();
    verify(provider, times(1)).validateArguments();
    verify(provider, times(1)).setChannelProvider(any(InstantiatingGrpcChannelProvider.class));
  }

  @Test
  public void testStopClose() throws Exception{
    DefaultChannelProvider provider = spy(new DefaultChannelProvider());
    LifecycleHelper.stopAndClose(provider);
    verify(provider, times(1)).stop();
    verify(provider, times(1)).close();
    verify(provider, never()).validateArguments();
    verify(provider, never()).setChannelProvider(any(InstantiatingGrpcChannelProvider.class));
  }

  @Test
  public void testCreateCredentialsProvider() throws Exception {
    DefaultChannelProvider provider = new DefaultChannelProvider();
    TransportChannelProvider credentialsProvider = provider.createChannelProvider();
    assertTrue(credentialsProvider instanceof InstantiatingGrpcChannelProvider);
  }

}