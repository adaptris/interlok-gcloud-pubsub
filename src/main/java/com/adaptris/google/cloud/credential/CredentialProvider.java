package com.adaptris.google.cloud.credential;


import com.adaptris.core.CoreException;
import com.google.auth.oauth2.GoogleCredentials;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;

public interface CredentialProvider {

  GoogleCredentials fromStreamWithScope(String jsonFile, Collection<String> scopes) throws CoreException;
}
