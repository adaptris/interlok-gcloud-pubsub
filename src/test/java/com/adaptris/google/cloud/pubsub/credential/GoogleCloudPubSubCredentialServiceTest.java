package com.adaptris.google.cloud.pubsub.credential;

import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.AdaptrisMessageFactory;
import com.adaptris.core.ConfiguredProduceDestination;
import com.adaptris.core.CoreException;
import com.adaptris.util.text.DateFormatUtil;
import org.junit.Test;

import java.io.File;
import java.net.URL;
import java.nio.file.Paths;
import java.util.Arrays;


public class GoogleCloudPubSubCredentialServiceTest extends CredentialServiceExample {

  public GoogleCloudPubSubCredentialServiceTest(String name) {
    super(name);
  }

  @Test
  public void testConstruct() throws Exception {
    GoogleCloudPubSubCredentialService service = new GoogleCloudPubSubCredentialService();
    assertNotNull(service.getAccessTokenKey());
    assertEquals(service.getAccessTokenKey(),GoogleCloudPubSubCredentialService.DEFAULT_ACCESS_TOKEN_KEY);
    assertNotNull(service.getAccessTokenExpirationKey());
    assertEquals(service.getAccessTokenExpirationKey(),GoogleCloudPubSubCredentialService.DEFAULT_ACCESS_TOKEN_EXPIRATION_KEY);
    assertNull(service.getScope());
    assertNull(service.getJsonKeyFile());
    assertNotNull(service.getCredentialWrapper());
    assertTrue(service.getCredentialWrapper() instanceof DefaultCredentialWrapper);
    service = new GoogleCloudPubSubCredentialService(new ConfiguredProduceDestination(), Arrays.asList("scope"));
    assertNotNull(service.getScope());
    assertTrue(service.getScope().contains("scope"));
    assertNotNull(service.getJsonKeyFile());
    assertTrue(service.getJsonKeyFile() instanceof ConfiguredProduceDestination);
  }

  @Test
  public void testInitFail() throws Exception {
    GoogleCloudPubSubCredentialService service = new GoogleCloudPubSubCredentialService();
    try {
      service.initService();
      fail();
    } catch (CoreException e){
      assertEquals("Value for json-key-file is invalid", e.getMessage());
    }
  }

  @Test
  public void testInitJsonKeyOnly() throws Exception {
    GoogleCloudPubSubCredentialService service = new GoogleCloudPubSubCredentialService();
    service.setJsonKeyFile(new ConfiguredProduceDestination());
    try {
      service.initService();
      fail();
    } catch (CoreException e){
      assertEquals("Value for scope is invalid", e.getMessage());
    }
  }

  @Test
  public void testInitScopeOnly() throws Exception {
    GoogleCloudPubSubCredentialService service = new GoogleCloudPubSubCredentialService();
    service.setScope(Arrays.asList("scope"));
    try {
      service.initService();
      fail();
    } catch (CoreException e){
      assertEquals("Value for json-key-file is invalid", e.getMessage());
    }
  }

  @Test
  public void testInitOk() throws Exception {
    GoogleCloudPubSubCredentialService service = new GoogleCloudPubSubCredentialService();
    service.setScope(Arrays.asList("scope"));
    service.setJsonKeyFile(new ConfiguredProduceDestination());
    service.initService();
    assertNotNull(service.getScope());
    assertTrue(service.getScope().contains("scope"));
    assertNotNull(service.getJsonKeyFile());
    assertTrue(service.getJsonKeyFile() instanceof ConfiguredProduceDestination);
  }

  @Test
  public void testDoService() throws Exception {
    AdaptrisMessage msg =  AdaptrisMessageFactory.getDefaultInstance().newMessage("Hello World");
    URL resource = GoogleCloudPubSubCredentialServiceTest.class.getClassLoader().getResource("interlok.json");
    File jsonFile = Paths.get(resource.toURI()).toFile();
    GoogleCloudPubSubCredentialService service = new GoogleCloudPubSubCredentialService();
    service.setCredentialWrapper(new StubCredentialWrapper());
    service.setJsonKeyFile(new ConfiguredProduceDestination("file:///" + jsonFile.getAbsolutePath()));
    service.setScope(Arrays.asList("https://www.googleapis.com/auth/pubsub"));
    service.doService(msg);
    assertTrue(msg.headersContainsKey(GoogleCloudPubSubCredentialService.DEFAULT_ACCESS_TOKEN_KEY));
    assertEquals(msg.getMetadataValue(GoogleCloudPubSubCredentialService.DEFAULT_ACCESS_TOKEN_KEY), StubCredentialWrapper.ACCESS_TOKEN);
    assertTrue(msg.headersContainsKey(GoogleCloudPubSubCredentialService.DEFAULT_ACCESS_TOKEN_EXPIRATION_KEY));
    assertEquals(msg.getMetadataValue(GoogleCloudPubSubCredentialService.DEFAULT_ACCESS_TOKEN_EXPIRATION_KEY), DateFormatUtil.format(StubCredentialWrapper.EXPIRATION));
  }

  @Override
  protected Object retrieveObjectForSampleConfig() {
    GoogleCloudPubSubCredentialService service = new GoogleCloudPubSubCredentialService();
    service.setScope(Arrays.asList("https://www.googleapis.com/auth/pubsub"));
    service.setJsonKeyFile(new ConfiguredProduceDestination("file:////home/matthew/interlok.json"));
    return service;
  }
}