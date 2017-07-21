package com.adaptris.google.cloud.pubsub.credentials;

import com.adaptris.core.ComponentLifecycle;
import com.adaptris.core.CoreException;

public abstract class CredentialsProvider implements ComponentLifecycle {

  private transient com.google.api.gax.core.CredentialsProvider credentialsProvider;

  abstract com.google.api.gax.core.CredentialsProvider createCredentialsProvider() throws CoreException;

  @Override
  public void init() throws CoreException {
    setCredentialsProvider(createCredentialsProvider());
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

  void setCredentialsProvider(com.google.api.gax.core.CredentialsProvider credentialsProvider) {
    this.credentialsProvider = credentialsProvider;
  }

  public com.google.api.gax.core.CredentialsProvider getCredentialsProvider() {
    return credentialsProvider;
  }
}
