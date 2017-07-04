package com.adaptris.google.cloud.pubsub.connection;

import com.adaptris.core.CoreException;
import com.adaptris.core.util.LifecycleHelper;
import com.adaptris.google.cloud.pubsub.connection.channel.ChannelProvider;
import com.adaptris.google.cloud.pubsub.connection.channel.CustomChannelProvider;
import com.adaptris.google.cloud.pubsub.connection.channel.DefaultChannelProvider;
import com.adaptris.google.cloud.pubsub.connection.credentials.CredentialsProvider;
import com.adaptris.google.cloud.pubsub.connection.credentials.KeyFileCredentialsProvider;
import com.adaptris.google.cloud.pubsub.connection.credentials.NoCredentialsProvider;
import com.google.api.gax.grpc.InstantiatingChannelProvider;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.*;

public class GoogleCloudPubSubConsumeConnectionTest {

  @Test
  public void testConstruct() throws Exception {
    GoogleCloudPubSubConsumeConnection connection = new GoogleCloudPubSubConsumeConnection();
    assertNull(connection.getProjectName());
    assertNotNull(connection.getChannelProvider());
    assertTrue(connection.getChannelProvider() instanceof DefaultChannelProvider);
    assertNotNull(connection.getCredentialsProvider());
    assertTrue(connection.getCredentialsProvider() instanceof NoCredentialsProvider);
    assertEquals(connection.getConnectionState(), ConsumerConnectionConfig.ConnectionState.Closed);
    connection = new GoogleCloudPubSubConsumeConnection(new CustomChannelProvider());
    assertTrue(connection.getChannelProvider() instanceof CustomChannelProvider);
    connection = new GoogleCloudPubSubConsumeConnection(new CustomChannelProvider(), new KeyFileCredentialsProvider());
    assertTrue(connection.getChannelProvider() instanceof CustomChannelProvider);
    assertTrue(connection.getCredentialsProvider() instanceof KeyFileCredentialsProvider);
  }

  @Test
  public void testPrepareConnection() throws Exception {
    GoogleCloudPubSubConsumeConnection connection = new GoogleCloudPubSubConsumeConnection();
    prepareFail(connection, "Project Name is invalid");
    connection.setProjectName("");
    prepareFail(connection, "Project Name is invalid");
    connection.setProjectName("project-name");
    connection.prepareConnection();
  }

  private void prepareFail(GoogleCloudPubSubConsumeConnection connection, String message){
    try {
      connection.prepareConnection();
      fail();
    } catch (CoreException expected){
      assertEquals(message, expected.getMessage());
    }
  }

  @Test
  public void testGetProjectName() throws Exception {
    GoogleCloudPubSubConsumeConnection connection = new GoogleCloudPubSubConsumeConnection();
    connection.setProjectName("project-name");
    assertEquals("project-name", connection.getProjectName());
  }

  @Test
  public void testLifeCycleInitStartAndGetGoogleProvider() throws Exception {
    GoogleCloudPubSubConsumeConnection connection = Mockito.spy(new GoogleCloudPubSubConsumeConnection());
    connection.setProjectName("project-name");
    CredentialsProvider credentialsProvider = Mockito.mock(NoCredentialsProvider.class);
    Mockito.doReturn(new com.google.api.gax.core.NoCredentialsProvider()).when(credentialsProvider).getCredentialsProvider();
    ChannelProvider channelProvider = Mockito.mock(DefaultChannelProvider.class);
    Mockito.doReturn(InstantiatingChannelProvider.newBuilder().build()).when(channelProvider).getChannelProvider();
    connection.setCredentialsProvider(credentialsProvider);
    connection.setChannelProvider(channelProvider);
    LifecycleHelper.initAndStart(connection);
    assertFalse(connection.getConnectionState().isStopOrClose());
    assertTrue(connection.getGoogleCredentialsProvider() instanceof com.google.api.gax.core.NoCredentialsProvider);
    assertTrue(connection.getGoogleChannelProvider()instanceof InstantiatingChannelProvider);
    LifecycleHelper.stopAndClose(connection);
    assertTrue(connection.getConnectionState().isStopOrClose());
    Mockito.verify(connection, Mockito.times(1)).initConnection();
    Mockito.verify(credentialsProvider, Mockito.times(1)).init();
    Mockito.verify(channelProvider, Mockito.times(1)).init();
    Mockito.verify(connection, Mockito.times(1)).startConnection();
    Mockito.verify(credentialsProvider, Mockito.times(1)).start();
    Mockito.verify(channelProvider, Mockito.times(1)).start();
    Mockito.verify(credentialsProvider, Mockito.times(1)).getCredentialsProvider();
    Mockito.verify(channelProvider, Mockito.times(1)).getChannelProvider();
    Mockito.verify(connection, Mockito.times(1)).stopConnection();
    Mockito.verify(credentialsProvider, Mockito.times(1)).stop();
    Mockito.verify(channelProvider, Mockito.times(1)).stop();
    Mockito.verify(connection, Mockito.times(1)).closeConnection();
    Mockito.verify(credentialsProvider, Mockito.times(1)).close();
    Mockito.verify(channelProvider, Mockito.times(1)).close();
  }

  @Test
  public void testConnectionStateIsStopOrClose(){
    assertFalse(ConsumerConnectionConfig.ConnectionState.Initialising.isStopOrClose());
    assertFalse(ConsumerConnectionConfig.ConnectionState.Initialised.isStopOrClose());
    assertFalse(ConsumerConnectionConfig.ConnectionState.Starting.isStopOrClose());
    assertFalse(ConsumerConnectionConfig.ConnectionState.Started.isStopOrClose());
    assertTrue(ConsumerConnectionConfig.ConnectionState.Stopping.isStopOrClose());
    assertTrue(ConsumerConnectionConfig.ConnectionState.Stopped.isStopOrClose());
    assertTrue(ConsumerConnectionConfig.ConnectionState.Closing.isStopOrClose());
    assertTrue(ConsumerConnectionConfig.ConnectionState.Closed.isStopOrClose());
  }

}