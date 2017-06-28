package com.adaptris.google.cloud.credential;


import com.google.auth.oauth2.GoogleCredentials;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;

interface CredentialWrapper {

  GoogleCredentials fromStreamWithScope(InputStream inputStream, Collection<String> scopes) throws IOException;
}
