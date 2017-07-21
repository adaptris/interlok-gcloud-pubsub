package com.adaptris.google.cloud.pubsub.credentials;

import com.adaptris.core.CoreException;
import com.adaptris.core.util.LifecycleHelper;
import com.adaptris.google.cloud.credential.Credentials;
import com.adaptris.google.cloud.credential.StubCredentials;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;


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
    Credentials credentials =  Mockito.spy(new StubCredentials());
    FixedCredentialsProvider provider = Mockito.spy(new FixedCredentialsProvider(credentials));
    LifecycleHelper.initAndStart(provider);
    assertTrue(provider.getCredentialsProvider() instanceof com.google.api.gax.core.FixedCredentialsProvider);
    Mockito.verify(provider,Mockito.times(1)).init();
    Mockito.verify(provider,Mockito.times(1)).start();
    Mockito.verify(provider,Mockito.times(1)).setCredentialsProvider(Mockito.any(com.google.api.gax.core.NoCredentialsProvider.class));
    Mockito.verify(credentials,Mockito.times(1)).init();
    Mockito.verify(credentials,Mockito.times(1)).start();
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
    Credentials credentials =  Mockito.spy(new StubCredentials());
    FixedCredentialsProvider provider = Mockito.spy(new FixedCredentialsProvider(credentials));
    LifecycleHelper.stopAndClose(provider);
    Mockito.verify(provider,Mockito.times(1)).stop();
    Mockito.verify(provider,Mockito.times(1)).close();
    Mockito.verify(provider,Mockito.never()).setCredentialsProvider(Mockito.any(com.google.api.gax.core.NoCredentialsProvider.class));
    Mockito.verify(credentials,Mockito.times(1)).stop();
    Mockito.verify(credentials,Mockito.times(1)).close();
  }

  @Test
  public void testCreateCredentialsProvider() throws Exception {
    FixedCredentialsProvider provider = new FixedCredentialsProvider(new StubCredentials());
    com.google.api.gax.core.CredentialsProvider credentialsProvider = provider.createCredentialsProvider();
    assertTrue(credentialsProvider instanceof com.google.api.gax.core.FixedCredentialsProvider);
  }

}