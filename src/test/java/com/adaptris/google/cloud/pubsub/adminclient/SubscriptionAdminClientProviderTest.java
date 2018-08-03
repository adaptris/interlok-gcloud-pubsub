package com.adaptris.google.cloud.pubsub.adminclient;

import com.adaptris.core.CoreException;
import com.adaptris.core.util.LifecycleHelper;
import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.grpc.GrpcCallContext;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.ApiCallContext;
import com.google.api.gax.rpc.TransportChannelProvider;
import io.grpc.ManagedChannel;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.*;

public class SubscriptionAdminClientProviderTest {

  @Test
  public void testLifeCycle() throws  Exception {
    SubscriptionAdminClientProvider adminClientProvider = new SubscriptionAdminClientProvider();
    TransportChannelProvider channelProvider = Mockito.mock(TransportChannelProvider.class);
    Mockito.doReturn("grpc").when(channelProvider).getTransportName();
    GrpcTransportChannel managedChannel = GrpcTransportChannel.create(Mockito.mock(ManagedChannel.class));
    Mockito.doReturn(managedChannel).when(channelProvider).getTransportChannel();
    CredentialsProvider credentialsProvider = Mockito.mock(CredentialsProvider.class);
    adminClientProvider.setChannelProvider(channelProvider);
    adminClientProvider.setCredentialsProvider(credentialsProvider);
    LifecycleHelper.initAndStart(adminClientProvider);
    assertNotNull(adminClientProvider.getSubscriptionAdminClient());
    LifecycleHelper.stopAndClose(adminClientProvider);
  }

  @Test
  public void testInit() throws  Exception {
    SubscriptionAdminClientProvider adminClientProvider = new SubscriptionAdminClientProvider();
    TransportChannelProvider channelProvider = Mockito.mock(TransportChannelProvider.class);
    Mockito.doReturn("grpc").when(channelProvider).getTransportName();
    GrpcTransportChannel managedChannel = GrpcTransportChannel.create(Mockito.mock(ManagedChannel.class));
    Mockito.doReturn(managedChannel).when(channelProvider).getTransportChannel();
    CredentialsProvider credentialsProvider = Mockito.mock(CredentialsProvider.class);
    initFail(adminClientProvider,"ChannelProvider can not be null");
    adminClientProvider.setChannelProvider(channelProvider);
    initFail(adminClientProvider,"CredentialsProvider can not be null");
    adminClientProvider.setCredentialsProvider(credentialsProvider);
    adminClientProvider.init();

  }

  private void initFail(SubscriptionAdminClientProvider provider, String message){
    try {
      provider.init();
      fail();
    } catch (CoreException expected){
      assertEquals(message, expected.getMessage());
    }
  }

}