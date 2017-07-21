package com.adaptris.google.cloud.pubsub.credentials;

import com.adaptris.core.util.LifecycleHelper;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertTrue;


public class NoCredentialsProviderTest {

  @Test
  public void testInit() throws Exception {
    NoCredentialsProvider provider = Mockito.spy(new NoCredentialsProvider());
    LifecycleHelper.initAndStart(provider);
    assertTrue(provider.getCredentialsProvider() instanceof com.google.api.gax.core.NoCredentialsProvider);
    Mockito.verify(provider,Mockito.times(1)).init();
    Mockito.verify(provider,Mockito.times(1)).start();
    Mockito.verify(provider,Mockito.times(1)).setCredentialsProvider(Mockito.any(com.google.api.gax.core.NoCredentialsProvider.class));
  }

  @Test
  public void testStopClose() throws Exception{
    NoCredentialsProvider provider = Mockito.spy(new NoCredentialsProvider());
    LifecycleHelper.stopAndClose(provider);
    Mockito.verify(provider,Mockito.times(1)).stop();
    Mockito.verify(provider,Mockito.times(1)).close();
    Mockito.verify(provider,Mockito.never()).setCredentialsProvider(Mockito.any(com.google.api.gax.core.NoCredentialsProvider.class));
  }

  @Test
  public void testCreateCredentialsProvider() throws Exception {
    NoCredentialsProvider provider = new NoCredentialsProvider();
    com.google.api.gax.core.CredentialsProvider credentialsProvider = provider.createCredentialsProvider();
    assertTrue(credentialsProvider instanceof com.google.api.gax.core.NoCredentialsProvider);
  }

}