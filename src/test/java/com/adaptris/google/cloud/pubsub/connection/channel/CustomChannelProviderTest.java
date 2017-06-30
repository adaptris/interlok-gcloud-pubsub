package com.adaptris.google.cloud.pubsub.connection.channel;

import com.adaptris.core.CoreException;
import com.adaptris.core.util.LifecycleHelper;
import com.adaptris.google.cloud.pubsub.connection.credentials.CredentialsProvider;
import com.adaptris.google.cloud.pubsub.connection.credentials.KeyFileCredentialsProvider;
import com.google.api.gax.grpc.FixedChannelProvider;
import com.google.api.gax.grpc.InstantiatingChannelProvider;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;

import static org.junit.Assert.*;

public class CustomChannelProviderTest {

  @Test
  public void testConstruct() throws Exception {
    CustomChannelProvider provider = new CustomChannelProvider();
    assertNull(provider.getAddress());
    assertTrue(provider.getUsePlaintext());
    provider = new CustomChannelProvider("localhost:9999");
    assertEquals("localhost:9999", provider.getAddress());
    assertTrue(provider.getUsePlaintext());
    provider = new CustomChannelProvider("localhost:9999", false);
    assertEquals("localhost:9999", provider.getAddress());
    assertFalse(provider.getUsePlaintext());
  }

  @Test
  public void testInit() throws Exception {
    CustomChannelProvider provider = Mockito.spy(new CustomChannelProvider());
    provider.setAddress("localhost:9999");
    provider.setUsePlaintext(true);
    LifecycleHelper.initAndStart(provider);
    assertTrue(provider.getChannelProvider() instanceof FixedChannelProvider);
    Mockito.verify(provider,Mockito.times(1)).init();
    Mockito.verify(provider,Mockito.times(1)).start();
    Mockito.verify(provider,Mockito.times(1)).validateArguments();
    Mockito.verify(provider,Mockito.times(1)).setChannelProvider(Mockito.any(FixedChannelProvider.class));
  }

  @Test
  public void testStopClose() throws Exception{
    CustomChannelProvider provider = Mockito.spy(new CustomChannelProvider());
    LifecycleHelper.stopAndClose(provider);
    Mockito.verify(provider,Mockito.times(1)).stop();
    Mockito.verify(provider,Mockito.times(1)).close();
    Mockito.verify(provider,Mockito.never()).validateArguments();
    Mockito.verify(provider,Mockito.never()).setChannelProvider(Mockito.any(FixedChannelProvider.class));
  }

  @Test
  public void testCreateCredentialsProvider() throws Exception {
    CustomChannelProvider provider = new CustomChannelProvider();
    provider.setAddress("localhost:9999");
    provider.setUsePlaintext(true);
    com.google.api.gax.grpc.ChannelProvider credentialsProvider = provider.createChannelProvider();
    assertTrue(credentialsProvider instanceof FixedChannelProvider);
  }

  @Test
  public void testAddress() throws Exception {
    CustomChannelProvider provider = new CustomChannelProvider();
    assertNull(provider.getAddress());
    provider.setAddress("localhost:9999");
    assertEquals("localhost:9999", provider.getAddress());
  }

  @Test
  public void testUsePlaintext() throws Exception {
    CustomChannelProvider provider = new CustomChannelProvider();
    assertNotNull(provider.getUsePlaintext());
    assertTrue(provider.getUsePlaintext());
    provider.setUsePlaintext(false);
    assertFalse(provider.getUsePlaintext());
  }

  @Test
  public void testValidateArguments() throws Exception {
    CustomChannelProvider provider = new CustomChannelProvider(null, null);
    validateArgumentsFail(provider, "Address is invalid");
    provider.setAddress("");
    validateArgumentsFail(provider, "Address is invalid");
    provider.setAddress("locahost:9999");
    validateArgumentsFail(provider, "Use Plaintext is invalid");
    provider.setUsePlaintext(false);
    provider.validateArguments();
  }

  private void validateArgumentsFail(CustomChannelProvider provider, String message){
    try {
      provider.validateArguments();
      fail();
    } catch (CoreException expected){
      assertEquals(message, expected.getMessage());
    }
  }

}