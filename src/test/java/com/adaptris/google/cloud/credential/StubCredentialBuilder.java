package com.adaptris.google.cloud.credential;


import com.adaptris.core.CoreException;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import org.mockito.Mockito;

import java.util.Collection;
import java.util.Date;

import static org.mockito.Mockito.mock;

class StubCredentialBuilder implements CredentialBuilder {

  static final String ACCESS_TOKEN = "ABC123";
  static final Date EXPIRATION = new Date(0);

  GoogleCredentials credentials;

  StubCredentialBuilder() throws Exception{
    credentials = mock(GoogleCredentials.class);
    Mockito.when(credentials.refreshAccessToken()).thenReturn(new AccessToken(ACCESS_TOKEN, EXPIRATION));
  }

  @Override
  public GoogleCredentials fromStreamWithScope(String jsonFile, Collection<String> scopes) throws CoreException {
    return credentials;
  }
}
