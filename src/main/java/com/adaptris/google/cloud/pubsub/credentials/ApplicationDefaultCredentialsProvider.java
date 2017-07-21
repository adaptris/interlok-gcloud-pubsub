package com.adaptris.google.cloud.pubsub.credentials;

import com.adaptris.core.CoreException;
import com.google.api.gax.core.GoogleCredentialsProvider;
import com.thoughtworks.xstream.annotations.XStreamAlias;

@XStreamAlias("application-default-credentials-provider")
public class ApplicationDefaultCredentialsProvider extends ScopedCredentialsProvider {

  @Override
  com.google.api.gax.core.CredentialsProvider createCredentialsProvider() throws CoreException {
    return GoogleCredentialsProvider.newBuilder().setScopesToApply(getScopes()).build();
  }

}
