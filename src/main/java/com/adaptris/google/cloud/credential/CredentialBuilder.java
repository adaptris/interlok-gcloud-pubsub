package com.adaptris.google.cloud.credential;


import com.adaptris.core.CoreException;
import com.google.auth.oauth2.GoogleCredentials;

import java.util.Collection;

public interface CredentialBuilder {

  GoogleCredentials fromStreamWithScope(String jsonFile, Collection<String> scopes) throws CoreException;
}
