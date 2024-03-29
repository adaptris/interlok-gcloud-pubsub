package com.adaptris.google.cloud.pubsub;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import com.adaptris.core.CoreException;
import com.adaptris.core.util.LifecycleHelper;
import com.adaptris.google.cloud.pubsub.adminclient.SubscriptionAdminClientProvider;
import com.adaptris.google.cloud.pubsub.adminclient.TopicAdminClientProvider;
import com.adaptris.google.cloud.pubsub.channel.ChannelProvider;
import com.adaptris.google.cloud.pubsub.channel.CustomChannelProvider;
import com.adaptris.google.cloud.pubsub.channel.DefaultChannelProvider;
import com.adaptris.google.cloud.pubsub.credentials.CredentialsProvider;
import com.adaptris.google.cloud.pubsub.credentials.FixedCredentialsProvider;
import com.adaptris.google.cloud.pubsub.credentials.NoCredentialsProvider;
import com.adaptris.google.cloud.pubsub.flowcontrol.CustomFlowControlProvider;
import com.adaptris.google.cloud.pubsub.flowcontrol.DefaultFlowControlProvider;
import com.google.api.gax.grpc.InstantiatingGrpcChannelProvider;

public class GoogleCloudPubSubConnectionTest {

  @Test
  public void testConstruct() throws Exception {
    GoogleCloudPubSubConnection connection = new GoogleCloudPubSubConnection();
    assertNull(connection.getProjectName());
    assertNotNull(connection.getFlowControlProvider());
    assertTrue(connection.getFlowControlProvider() instanceof DefaultFlowControlProvider);
    assertNotNull(connection.getChannelProvider());
    assertTrue(connection.getChannelProvider() instanceof DefaultChannelProvider);
    assertNotNull(connection.getCredentialsProvider());
    assertTrue(connection.getCredentialsProvider() instanceof NoCredentialsProvider);
    assertEquals(connection.getConnectionState(), ConnectionConfig.ConnectionState.Closed);
    connection = new GoogleCloudPubSubConnection(new CustomChannelProvider());
    assertTrue(connection.getChannelProvider() instanceof CustomChannelProvider);
    connection = new GoogleCloudPubSubConnection(new CustomChannelProvider(), new FixedCredentialsProvider());
    assertTrue(connection.getChannelProvider() instanceof CustomChannelProvider);
    assertTrue(connection.getCredentialsProvider() instanceof FixedCredentialsProvider);
  }

  @Test
  public void testPrepareConnection() throws Exception {
    GoogleCloudPubSubConnection connection = new GoogleCloudPubSubConnection();
    prepareFail(connection, "Project Name is invalid");
    connection.setProjectName("");
    prepareFail(connection, "Project Name is invalid");
    connection.setProjectName("project-name");
    connection.prepareConnection();
  }

  private void prepareFail(GoogleCloudPubSubConnection connection, String message) {
    try {
      connection.prepareConnection();
      fail();
    } catch (CoreException expected) {
      assertEquals(message, expected.getMessage());
    }
  }

  @Test
  public void testGetProjectName() throws Exception {
    GoogleCloudPubSubConnection connection = new GoogleCloudPubSubConnection();
    connection.setProjectName("project-name");
    assertEquals("project-name", connection.getProjectName());
  }

  @Test
  public void testLifeCycleInitStartAndGetGoogleProvider() throws Exception {
    GoogleCloudPubSubConnection connection = Mockito.spy(new GoogleCloudPubSubConnection());
    connection.setProjectName("project-name");
    CredentialsProvider credentialsProvider = Mockito.mock(NoCredentialsProvider.class);
    Mockito.doReturn(new com.google.api.gax.core.NoCredentialsProvider()).when(credentialsProvider).getCredentialsProvider();
    ChannelProvider channelProvider = Mockito.mock(DefaultChannelProvider.class);
    Mockito.doReturn(InstantiatingGrpcChannelProvider.newBuilder().build()).when(channelProvider).getChannelProvider();
    SubscriptionAdminClientProvider subscriptionAdminClientProvider = Mockito.mock(SubscriptionAdminClientProvider.class);
    TopicAdminClientProvider topicAdminClientProvider = Mockito.mock(TopicAdminClientProvider.class);
    connection.setSubscriptionAdminClientProvider(subscriptionAdminClientProvider);
    connection.setTopicAdminClientProvider(topicAdminClientProvider);
    connection.setCredentialsProvider(credentialsProvider);
    connection.setChannelProvider(channelProvider);
    LifecycleHelper.initAndStart(connection);
    assertFalse(connection.getConnectionState().isStopOrClose());
    assertTrue(connection.getGoogleCredentialsProvider() instanceof com.google.api.gax.core.NoCredentialsProvider);
    assertTrue(connection.getGoogleChannelProvider() instanceof InstantiatingGrpcChannelProvider);
    LifecycleHelper.stopAndClose(connection);
    assertTrue(connection.getConnectionState().isStopOrClose());
    Mockito.verify(connection, Mockito.times(1)).initConnection();
    Mockito.verify(credentialsProvider, Mockito.times(1)).init();
    Mockito.verify(channelProvider, Mockito.times(1)).init();
    Mockito.verify(subscriptionAdminClientProvider, Mockito.times(1)).init();
    Mockito.verify(topicAdminClientProvider, Mockito.times(1)).init();
    Mockito.verify(connection, Mockito.times(1)).startConnection();
    Mockito.verify(credentialsProvider, Mockito.times(1)).start();
    Mockito.verify(channelProvider, Mockito.times(1)).start();
    Mockito.verify(subscriptionAdminClientProvider, Mockito.times(1)).start();
    Mockito.verify(topicAdminClientProvider, Mockito.times(1)).start();
    Mockito.verify(credentialsProvider, Mockito.atLeastOnce()).getCredentialsProvider();
    Mockito.verify(channelProvider, Mockito.atLeastOnce()).getChannelProvider();
    Mockito.verify(connection, Mockito.times(1)).stopConnection();
    Mockito.verify(credentialsProvider, Mockito.times(1)).stop();
    Mockito.verify(channelProvider, Mockito.times(1)).stop();
    Mockito.verify(subscriptionAdminClientProvider, Mockito.times(1)).stop();
    Mockito.verify(topicAdminClientProvider, Mockito.times(1)).stop();
    Mockito.verify(connection, Mockito.times(1)).closeConnection();
    Mockito.verify(credentialsProvider, Mockito.times(1)).close();
    Mockito.verify(channelProvider, Mockito.times(1)).close();
    Mockito.verify(subscriptionAdminClientProvider, Mockito.times(1)).close();
    Mockito.verify(topicAdminClientProvider, Mockito.times(1)).close();
  }

  @Test
  public void testConnectionStateIsStopOrClose() {
    assertFalse(ConnectionConfig.ConnectionState.Initialising.isStopOrClose());
    assertFalse(ConnectionConfig.ConnectionState.Initialised.isStopOrClose());
    assertFalse(ConnectionConfig.ConnectionState.Starting.isStopOrClose());
    assertFalse(ConnectionConfig.ConnectionState.Started.isStopOrClose());
    assertTrue(ConnectionConfig.ConnectionState.Stopping.isStopOrClose());
    assertTrue(ConnectionConfig.ConnectionState.Stopped.isStopOrClose());
    assertTrue(ConnectionConfig.ConnectionState.Closing.isStopOrClose());
    assertTrue(ConnectionConfig.ConnectionState.Closed.isStopOrClose());
  }

  @Test
  public void testGetSubscriptionAdminClient() {
    GoogleCloudPubSubConnection connection = new GoogleCloudPubSubConnection();
    SubscriptionAdminClientProvider provider = Mockito.mock(SubscriptionAdminClientProvider.class);
    connection.setSubscriptionAdminClientProvider(provider);
    connection.getSubscriptionAdminClient();
    Mockito.verify(provider, Mockito.times(1)).getSubscriptionAdminClient();
  }

  @Test
  public void testGetTopicAdminClient() {
    GoogleCloudPubSubConnection connection = new GoogleCloudPubSubConnection();
    TopicAdminClientProvider provider = Mockito.mock(TopicAdminClientProvider.class);
    connection.setTopicAdminClientProvider(provider);
    connection.getTopicAdminClient();
    Mockito.verify(provider, Mockito.times(1)).getTopicAdminClient();
  }

  @Test
  public void testGetFlowControlProvider() {
    GoogleCloudPubSubConnection connection = new GoogleCloudPubSubConnection();
    assertNotNull(connection.getFlowControlProvider());
    assertTrue(connection.getFlowControlProvider() instanceof DefaultFlowControlProvider);
    connection.setFlowControlProvider(new CustomFlowControlProvider());
    assertNotNull(connection.getFlowControlProvider());
    assertTrue(connection.getFlowControlProvider() instanceof CustomFlowControlProvider);
  }

}