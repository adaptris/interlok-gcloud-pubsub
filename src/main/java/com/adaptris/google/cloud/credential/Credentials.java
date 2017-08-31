package com.adaptris.google.cloud.credential;


import com.adaptris.core.ComponentLifecycle;
import com.adaptris.core.CoreException;
import com.google.auth.oauth2.GoogleCredentials;

import java.util.Collection;

public interface Credentials extends ComponentLifecycle {

  GoogleCredentials build() throws CoreException;
}
