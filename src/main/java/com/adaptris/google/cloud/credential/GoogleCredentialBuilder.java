package com.adaptris.google.cloud.credential;


import com.adaptris.core.CoreException;
import com.adaptris.core.fs.FsHelper;
import com.google.auth.oauth2.GoogleCredentials;

import java.io.*;
import java.net.URL;
import java.util.Collection;

public class GoogleCredentialBuilder implements CredentialBuilder {

  @Override
  public GoogleCredentials fromStreamWithScope(String jsonFile, Collection<String> scopes) throws CoreException {
    try {
      URL url = FsHelper.createUrlFromString(jsonFile, true);
      File jsonKey = FsHelper.createFileReference(url);
      return GoogleCredentials.fromStream(new FileInputStream(jsonKey)).createScoped(scopes);
    } catch (Exception e) {
      throw new CoreException(e);
    }
  }
}
