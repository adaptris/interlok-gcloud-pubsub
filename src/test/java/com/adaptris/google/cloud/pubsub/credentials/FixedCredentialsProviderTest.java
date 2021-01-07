package com.adaptris.google.cloud.pubsub.credentials;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.junit.Test;

import com.adaptris.core.CoreException;
import com.adaptris.core.oauth.gcloud.Credentials;
import com.adaptris.core.util.LifecycleHelper;
import com.adaptris.google.cloud.pubsub.stubs.StubCredentials;


public class FixedCredentialsProviderTest {

  @Test
  public void testConstruct() throws Exception {
    FixedCredentialsProvider provider = new FixedCredentialsProvider();
    assertNull(provider.getCredentials());
    provider = new FixedCredentialsProvider(new StubCredentials());

    assertNotNull(provider.getCredentials());
    assertTrue(provider.getCredentials() instanceof StubCredentials);
  }

  @Test
  public void testInit() throws Exception {
    Credentials credentials = spy(new StubCredentials());
    FixedCredentialsProvider provider = spy(new FixedCredentialsProvider(credentials));
    LifecycleHelper.initAndStart(provider);

    assertTrue(provider.getCredentialsProvider() instanceof com.google.api.gax.core.FixedCredentialsProvider);
    verify(provider, times(1)).init();
    verify(provider, times(1)).start();
    verify(provider, times(1)).setCredentialsProvider(any(com.google.api.gax.core.FixedCredentialsProvider.class));
    verify(credentials, times(1)).init();
    verify(credentials, times(1)).start();
  }

  @Test
  public void testInitFail() throws Exception {
    FixedCredentialsProvider provider = new FixedCredentialsProvider();
    try {
      provider.init();
      fail();
    } catch (CoreException expected){
      assertEquals("credentials is invalid", expected.getMessage());
    }
  }

  @Test
  public void testStopClose() throws Exception{
    Credentials credentials = spy(new StubCredentials());
    FixedCredentialsProvider provider = spy(new FixedCredentialsProvider(credentials));
    LifecycleHelper.stopAndClose(provider);

    verify(provider, times(1)).stop();
    verify(provider, times(1)).close();
    verify(provider, never()).setCredentialsProvider(any(com.google.api.gax.core.FixedCredentialsProvider.class));
    verify(credentials, times(1)).stop();
    verify(credentials, times(1)).close();
  }

  @Test
  public void testCreateCredentialsProvider() throws Exception {
    FixedCredentialsProvider provider = new FixedCredentialsProvider(new StubCredentials());
    com.google.api.gax.core.CredentialsProvider credentialsProvider = provider.createCredentialsProvider();

    assertTrue(credentialsProvider instanceof com.google.api.gax.core.FixedCredentialsProvider);
  }

}