package com.adaptris.google.cloud.pubsub.credential;


import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import org.mockito.Mockito;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Date;

import static org.mockito.Mockito.mock;

class StubCredentialWrapper implements CredentialWrapper {

  static final String ACCESS_TOKEN = "ABC123";
  static final Date EXPIRATION = new Date(0);

  GoogleCredentials credentials;

  StubCredentialWrapper() throws Exception{
    credentials = mock(GoogleCredentials.class);
    Mockito.when(credentials.refreshAccessToken()).thenReturn(new AccessToken(ACCESS_TOKEN, EXPIRATION));
  }

  @Override
  public GoogleCredentials fromStreamWithScope(InputStream inputStream, Collection<String> scopes) throws IOException {
    return credentials;
  }
}
