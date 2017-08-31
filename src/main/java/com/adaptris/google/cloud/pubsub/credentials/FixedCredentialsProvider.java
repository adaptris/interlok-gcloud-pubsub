package com.adaptris.google.cloud.pubsub.credentials;


import com.adaptris.core.CoreException;
import com.adaptris.google.cloud.credential.ApplicationDefaultCredentials;
import com.adaptris.google.cloud.credential.Credentials;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import org.apache.commons.lang.StringUtils;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

@XStreamAlias("fixed-credentials-provider")
public class FixedCredentialsProvider extends CredentialsProvider {

  @Valid
  @NotNull
  private Credentials credentials;

  public FixedCredentialsProvider(){
  }

  public FixedCredentialsProvider(Credentials credentials){
    setCredentials(credentials);
  }

  @Override
  com.google.api.gax.core.CredentialsProvider createCredentialsProvider() throws CoreException {
    return com.google.api.gax.core.FixedCredentialsProvider.create(getCredentials().build());
  }

  @Override
  public void init() throws CoreException {
    if (getCredentials() == null){
      throw new CoreException("credentials is invalid");
    }
    getCredentials().init();
    super.init();
  }

  @Override
  public void start() throws CoreException {
    getCredentials().start();
  }

  @Override
  public void stop() {
    getCredentials().stop();
  }

  @Override
  public void close() {
    getCredentials().close();
  }

  public void setCredentials(Credentials credentials) {
    this.credentials = credentials;
  }

  public Credentials getCredentials() {
    return credentials;
  }
}
