package com.adaptris.google.cloud.pubsub;

import com.adaptris.core.util.LifecycleHelper;
import org.junit.Test;
import org.mockito.Mockito;

public class GoogleCloudPubSubConnectionErrorHandlerTest {

  @Test
  public void testLifecycle() throws Exception{
    GoogleCloudPubSubConnectionErrorHandler errorHandler = new GoogleCloudPubSubConnectionErrorHandler();
    LifecycleHelper.initAndStart(errorHandler);
    LifecycleHelper.stopAndClose(errorHandler);
  }

  //This test probably should check something but it doesn't..
  @Test
  public void testHandleConnectionException() throws Exception {
    GoogleCloudPubSubConnectionErrorHandler errorHandler = Mockito.spy(new GoogleCloudPubSubConnectionErrorHandler());
    GoogleCloudPubSubConnection connection = new GoogleCloudPubSubConnection();
    connection.setConnectionState(ConnectionConfig.ConnectionState.Started);
    errorHandler.registerConnection(connection);
    errorHandler.handleConnectionException();
    connection.setConnectionState(ConnectionConfig.ConnectionState.Closed);
    errorHandler.handleConnectionException();
  }

}