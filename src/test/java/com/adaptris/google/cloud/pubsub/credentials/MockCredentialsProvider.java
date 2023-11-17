package com.adaptris.google.cloud.pubsub.credentials;

import com.adaptris.core.CoreException;

public class MockCredentialsProvider extends CredentialsProvider {

  private transient com.google.api.gax.core.CredentialsProvider credentialsProvider;

  public MockCredentialsProvider(com.google.api.gax.core.CredentialsProvider credentialsProvider) {
    this.credentialsProvider = credentialsProvider;
  }

  @Override
  com.google.api.gax.core.CredentialsProvider createCredentialsProvider() throws CoreException {
    return credentialsProvider;
  }

}
