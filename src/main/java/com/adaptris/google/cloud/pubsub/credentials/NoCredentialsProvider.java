package com.adaptris.google.cloud.pubsub.credentials;


import com.adaptris.core.CoreException;
import com.thoughtworks.xstream.annotations.XStreamAlias;

/**
 * @config no-credentials-provider
 */
@XStreamAlias("no-credentials-provider")
public class NoCredentialsProvider extends CredentialsProvider {

  @Override
  com.google.api.gax.core.CredentialsProvider createCredentialsProvider() throws CoreException {
    return new com.google.api.gax.core.NoCredentialsProvider();
  }

}
