package com.adaptris.google.cloud.pubsub.stubs;


import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Date;

import com.adaptris.core.CoreException;
import com.adaptris.core.oauth.gcloud.Credentials;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;

public class StubCredentials implements Credentials {

  static final String ACCESS_TOKEN = "ABC123";
  static final Date EXPIRATION = new Date(0);

  GoogleCredentials credentials;

  public StubCredentials() throws Exception{
    credentials = mock(GoogleCredentials.class);
    when(credentials.refreshAccessToken()).thenReturn(new AccessToken(ACCESS_TOKEN, EXPIRATION));
  }

  @Override
  public GoogleCredentials build() throws CoreException {
    return credentials;
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
