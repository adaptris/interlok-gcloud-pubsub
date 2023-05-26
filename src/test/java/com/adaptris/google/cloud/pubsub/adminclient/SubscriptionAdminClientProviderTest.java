package com.adaptris.google.cloud.pubsub.adminclient;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import org.junit.jupiter.api.Test;

import com.adaptris.core.CoreException;
import com.adaptris.core.util.LifecycleHelper;
import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.TransportChannelProvider;

import io.grpc.ManagedChannel;

public class SubscriptionAdminClientProviderTest {

  @Test
  public void testLifeCycle() throws Exception {
    SubscriptionAdminClientProvider adminClientProvider = new SubscriptionAdminClientProvider();
    TransportChannelProvider channelProvider = mock(TransportChannelProvider.class);
    doReturn("grpc").when(channelProvider).getTransportName();
    GrpcTransportChannel managedChannel = GrpcTransportChannel.create(mock(ManagedChannel.class));
    doReturn(managedChannel).when(channelProvider).getTransportChannel();
    CredentialsProvider credentialsProvider = mock(CredentialsProvider.class);
    adminClientProvider.setChannelProvider(channelProvider);
    adminClientProvider.setCredentialsProvider(credentialsProvider);
    LifecycleHelper.initAndStart(adminClientProvider);
    assertNotNull(adminClientProvider.getSubscriptionAdminClient());
    LifecycleHelper.stopAndClose(adminClientProvider);
  }

  @Test
  public void testInit() throws Exception {
    SubscriptionAdminClientProvider adminClientProvider = new SubscriptionAdminClientProvider();
    TransportChannelProvider channelProvider = mock(TransportChannelProvider.class);
    doReturn("grpc").when(channelProvider).getTransportName();
    GrpcTransportChannel managedChannel = GrpcTransportChannel.create(mock(ManagedChannel.class));
    doReturn(managedChannel).when(channelProvider).getTransportChannel();
    CredentialsProvider credentialsProvider = mock(CredentialsProvider.class);
    initFail(adminClientProvider, "ChannelProvider can not be null");
    adminClientProvider.setChannelProvider(channelProvider);
    initFail(adminClientProvider, "CredentialsProvider can not be null");
    adminClientProvider.setCredentialsProvider(credentialsProvider);
    adminClientProvider.init();

  }

  private void initFail(SubscriptionAdminClientProvider provider, String message) {
    try {
      provider.init();
      fail();
    } catch (CoreException expected) {
      assertEquals(message, expected.getMessage());
    }
  }

}