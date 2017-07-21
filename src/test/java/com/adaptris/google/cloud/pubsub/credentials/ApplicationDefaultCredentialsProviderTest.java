package com.adaptris.google.cloud.pubsub.credentials;

import com.adaptris.core.CoreException;
import com.adaptris.core.util.LifecycleHelper;
import com.google.api.gax.core.GoogleCredentialsProvider;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;

import static org.junit.Assert.*;


public class ApplicationDefaultCredentialsProviderTest {

  @Test
  public void testInitStart() throws Exception{
    ApplicationDefaultCredentialsProvider provider = Mockito.spy(new ApplicationDefaultCredentialsProvider());
    provider.setScopes(Arrays.asList("scope"));
    LifecycleHelper.initAndStart(provider);
    assertTrue(provider.getCredentialsProvider() instanceof GoogleCredentialsProvider);
    Mockito.verify(provider,Mockito.times(1)).init();
    Mockito.verify(provider,Mockito.times(1)).start();
    Mockito.verify(provider,Mockito.times(1)).validateArguments();
    Mockito.verify(provider,Mockito.times(1)).setCredentialsProvider(Mockito.any(com.google.api.gax.core.NoCredentialsProvider.class));
  }

  @Test
  public void testStopClose() throws Exception{
    ApplicationDefaultCredentialsProvider provider = Mockito.spy(new ApplicationDefaultCredentialsProvider());
    provider.setScopes(Arrays.asList("scope"));
    LifecycleHelper.stopAndClose(provider);
    Mockito.verify(provider,Mockito.times(1)).stop();
    Mockito.verify(provider,Mockito.times(1)).close();
    Mockito.verify(provider,Mockito.never()).validateArguments();
    Mockito.verify(provider,Mockito.never()).setCredentialsProvider(Mockito.any(com.google.api.gax.core.NoCredentialsProvider.class));
  }

  @Test
  public void testValidateArguments() throws Exception {
    ApplicationDefaultCredentialsProvider provider = new ApplicationDefaultCredentialsProvider();
    validateArgumentsFail(provider, "Scope is invalid");
    provider.setScopes(new ArrayList<String>());
    validateArgumentsFail(provider, "Scope is invalid");
    provider.setScopes(Arrays.asList("scope"));
    provider.validateArguments();
  }

  private void validateArgumentsFail(CredentialsProvider provider, String message){
    try {
      provider.validateArguments();
      fail();
    } catch (CoreException expected){
      assertEquals(message, expected.getMessage());
    }
  }

  @Test
  public void testGetScopes() throws Exception {
    ApplicationDefaultCredentialsProvider provider = new ApplicationDefaultCredentialsProvider();
    provider.setScopes(Arrays.asList("scope"));
    assertEquals(1, provider.getScopes().size());
    assertEquals("scope", provider.getScopes().get(0));
  }

}