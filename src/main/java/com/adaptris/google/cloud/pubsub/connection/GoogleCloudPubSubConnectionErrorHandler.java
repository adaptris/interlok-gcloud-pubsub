package com.adaptris.google.cloud.pubsub.connection;

import com.adaptris.core.ConnectionErrorHandlerImp;
import com.adaptris.core.CoreException;
import com.thoughtworks.xstream.annotations.XStreamAlias;

@XStreamAlias("google-cloud-pubsub-connection-error-handler")
public class GoogleCloudPubSubConnectionErrorHandler extends ConnectionErrorHandlerImp {

  @Override
  public void handleConnectionException() {
    GoogleCloudPubSubConnection connection = retrieveConnection(GoogleCloudPubSubConnection.class);
    if(!connection.getConnectionState().isStopOrClose()) {
      super.restartAffectedComponents();
    }
  }

  @Override
  public void init() throws CoreException {

  }

  @Override
  public void start() throws CoreException {

  }

  @Override
  public void stop() {

  }

  @Override
  public void close() {

  }
}
