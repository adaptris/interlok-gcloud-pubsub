package com.adaptris.google.cloud.pubsub.adminclient;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminSettings;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.adaptris.core.CoreException;
import com.adaptris.core.util.LifecycleHelper;
import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.TransportChannelProvider;

import io.grpc.ManagedChannel;

import java.io.IOException;

public class TopicAdminClientProviderTest {

  private TopicAdminClientProvider adminClientProvider;
  private TransportChannelProvider channelProvider;
  private CredentialsProvider credentialsProvider;
  private static final TopicAdminClient topicAdminClient = mock(TopicAdminClient.class);

  @BeforeAll
  public static void setUpClass() throws Exception {
    mockStatic(TopicAdminClient.class);
    when(TopicAdminClient.create(any(TopicAdminSettings.class))).thenReturn(topicAdminClient);
  }

  @BeforeEach
  public void setUp() throws IOException {
    adminClientProvider = new TopicAdminClientProvider();
    channelProvider = mock(TransportChannelProvider.class);
    doReturn("grpc").when(channelProvider).getTransportName();
    doReturn(false).when(channelProvider).needsMtlsEndpoint();
    GrpcTransportChannel managedChannel = GrpcTransportChannel.create(mock(ManagedChannel.class));
    doReturn(managedChannel).when(channelProvider).getTransportChannel();
    credentialsProvider = mock(CredentialsProvider.class);
  }

  @Test
  public void testLifeCycle() throws Exception {
    adminClientProvider.setChannelProvider(channelProvider);
    adminClientProvider.setCredentialsProvider(credentialsProvider);
    LifecycleHelper.initAndStart(adminClientProvider);
    assertNotNull(adminClientProvider.getTopicAdminClient());
    LifecycleHelper.stopAndClose(adminClientProvider);
  }

  @Test
  public void testInit() throws Exception {
    assertNotNull(channelProvider, "TransportChannelProvider should not be null");
    initFail(adminClientProvider, "ChannelProvider can not be null");
    adminClientProvider.setChannelProvider(channelProvider);

    assertNotNull(credentialsProvider, "CredentialsProvider should not be null");
    initFail(adminClientProvider, "CredentialsProvider can not be null");
    adminClientProvider.setCredentialsProvider(credentialsProvider);

    adminClientProvider.init();
  }

  private void initFail(TopicAdminClientProvider provider, String message) {
    try {
      provider.init();
      fail();
    } catch (CoreException expected) {
      assertEquals(message, expected.getMessage());
    }
  }

}