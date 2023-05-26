package com.adaptris.google.cloud.pubsub.channel;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.junit.jupiter.api.Test;

import com.adaptris.core.CoreException;
import com.adaptris.core.util.LifecycleHelper;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.api.gax.rpc.TransportChannelProvider;

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
    CustomChannelProvider provider = spy(new CustomChannelProvider());
    provider.setAddress("localhost:9999");
    provider.setUsePlaintext(true);
    LifecycleHelper.initAndStart(provider);
    assertTrue(provider.getChannelProvider() instanceof FixedTransportChannelProvider);
    verify(provider, times(1)).init();
    verify(provider, times(1)).start();
    verify(provider, times(1)).validateArguments();
    verify(provider, times(1)).setChannelProvider(any(FixedTransportChannelProvider.class));
  }

  @Test
  public void testStopClose() throws Exception {
    CustomChannelProvider provider = spy(new CustomChannelProvider());
    LifecycleHelper.stopAndClose(provider);
    verify(provider, times(1)).stop();
    verify(provider, times(1)).close();
    verify(provider, never()).validateArguments();
    verify(provider, never()).setChannelProvider(any(FixedTransportChannelProvider.class));
  }

  @Test
  public void testCreateCredentialsProvider() throws Exception {
    CustomChannelProvider provider = new CustomChannelProvider();
    provider.setAddress("localhost:9999");
    provider.setUsePlaintext(true);
    TransportChannelProvider credentialsProvider = provider.createChannelProvider();
    assertTrue(credentialsProvider instanceof FixedTransportChannelProvider);
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

  private void validateArgumentsFail(CustomChannelProvider provider, String message) {
    try {
      provider.validateArguments();
      fail();
    } catch (CoreException expected) {
      assertEquals(message, expected.getMessage());
    }
  }

}