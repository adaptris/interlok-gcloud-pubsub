package com.adaptris.google.cloud.pubsub.credential;


import com.google.auth.oauth2.GoogleCredentials;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;

class DefaultCredentialWrapper implements CredentialWrapper {

  @Override
  public GoogleCredentials fromStreamWithScope(InputStream inputStream, Collection<String> scopes) throws IOException {
    return GoogleCredentials.fromStream(inputStream).createScoped(scopes);
  }
}
