package com.adaptris.google.cloud.pubsub.credentials;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.junit.jupiter.api.Test;

import com.adaptris.core.util.LifecycleHelper;

public class NoCredentialsProviderTest {

  @Test
  public void testInit() throws Exception {
    NoCredentialsProvider provider = spy(new NoCredentialsProvider());
    LifecycleHelper.initAndStart(provider);
    assertTrue(provider.getCredentialsProvider() instanceof com.google.api.gax.core.NoCredentialsProvider);
    verify(provider, times(1)).init();
    verify(provider, times(1)).start();
    verify(provider, times(1)).setCredentialsProvider(any(com.google.api.gax.core.NoCredentialsProvider.class));
  }

  @Test
  public void testStopClose() throws Exception {
    NoCredentialsProvider provider = spy(new NoCredentialsProvider());
    LifecycleHelper.stopAndClose(provider);
    verify(provider, times(1)).stop();
    verify(provider, times(1)).close();
    verify(provider, never()).setCredentialsProvider(any(com.google.api.gax.core.NoCredentialsProvider.class));
  }

  @Test
  public void testCreateCredentialsProvider() throws Exception {
    NoCredentialsProvider provider = new NoCredentialsProvider();
    com.google.api.gax.core.CredentialsProvider credentialsProvider = provider.createCredentialsProvider();
    assertTrue(credentialsProvider instanceof com.google.api.gax.core.NoCredentialsProvider);
  }

}