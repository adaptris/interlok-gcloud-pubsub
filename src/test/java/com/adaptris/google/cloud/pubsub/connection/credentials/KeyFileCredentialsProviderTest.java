package com.adaptris.google.cloud.pubsub.connection.credentials;

import com.adaptris.core.CoreException;
import com.adaptris.core.util.LifecycleHelper;
import com.google.api.gax.core.FixedCredentialsProvider;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


public class KeyFileCredentialsProviderTest {

  @Test
  public void testInitStart() throws Exception{
    KeyFileCredentialsProvider provider = Mockito.spy(new KeyFileCredentialsProvider());
    File file = new File(KeyFileCredentialsProviderTest.class.getClassLoader().getResource("interlok.json").getFile());
    provider.setJsonKeyFile("file:///" + file.getAbsolutePath());
    provider.setScopes(Arrays.asList("scope"));
    LifecycleHelper.initAndStart(provider);
    assertTrue(provider.getCredentialsProvider() instanceof FixedCredentialsProvider);
    Mockito.verify(provider,Mockito.times(1)).init();
    Mockito.verify(provider,Mockito.times(1)).start();
    Mockito.verify(provider,Mockito.times(1)).validateArguments();
    Mockito.verify(provider,Mockito.times(1)).setCredentialsProvider(Mockito.any(com.google.api.gax.core.NoCredentialsProvider.class));
  }

  @Test
  public void testStopClose() throws Exception{
    KeyFileCredentialsProvider provider = Mockito.spy(new KeyFileCredentialsProvider());
    File file = new File(KeyFileCredentialsProviderTest.class.getClassLoader().getResource("interlok.json").getFile());
    provider.setJsonKeyFile("file:///" + file.getAbsolutePath());
    provider.setScopes(Arrays.asList("scope"));
    LifecycleHelper.stopAndClose(provider);
    Mockito.verify(provider,Mockito.times(1)).stop();
    Mockito.verify(provider,Mockito.times(1)).close();
    Mockito.verify(provider,Mockito.never()).validateArguments();
    Mockito.verify(provider,Mockito.never()).setCredentialsProvider(Mockito.any(com.google.api.gax.core.NoCredentialsProvider.class));
  }

  @Test
  public void testValidateArguments() throws Exception {
    KeyFileCredentialsProvider provider = new KeyFileCredentialsProvider();
    validateArgumentsFail(provider, "Json Key File is invalid");
    provider.setJsonKeyFile("");
    validateArgumentsFail(provider, "Json Key File is invalid");
    provider.setJsonKeyFile("file:////opt/interlok/config/interlok.json");
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
  public void testGetJsonKeyFile() throws Exception {
    KeyFileCredentialsProvider provider = new KeyFileCredentialsProvider();
    provider.setJsonKeyFile("/opt/interlok/file.json");
    assertEquals("/opt/interlok/file.json", provider.getJsonKeyFile());
  }

  @Test
  public void testGetScopes() throws Exception {
    KeyFileCredentialsProvider provider = new KeyFileCredentialsProvider();
    provider.setScopes(Arrays.asList("scope"));
    assertEquals(1, provider.getScopes().size());
    assertEquals("scope", provider.getScopes().get(0));
  }

}