package com.adaptris.google.cloud.pubsub.connection;

import com.adaptris.core.ConnectionErrorHandlerImp;
import com.adaptris.core.CoreException;

public class GoogleCloudPubSubConnectionErrorHandler extends ConnectionErrorHandlerImp {

  @Override
  public void handleConnectionException() {
    super.restartAffectedComponents();
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
