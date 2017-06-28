package com.adaptris.google.cloud.credential;

import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.AdaptrisMessageFactory;
import com.adaptris.core.ConfiguredProduceDestination;
import com.adaptris.core.CoreException;
import com.adaptris.core.http.oauth.AccessToken;
import com.adaptris.core.http.oauth.GetOauthToken;
import com.adaptris.util.text.DateFormatUtil;
import org.junit.Test;

import java.io.File;
import java.net.URL;
import java.nio.file.Paths;
import java.util.Arrays;


public class GoogleCloudAccessTokenBuilderTest extends CredentialServiceExample {

  public GoogleCloudAccessTokenBuilderTest(String name) {
    super(name);
  }

  @Test
  public void testConstruct() throws Exception {
    GoogleCloudAccessTokenBuilder service = new GoogleCloudAccessTokenBuilder();
    assertNull(service.getScope());
    assertNull(service.getJsonKeyFile());
    assertNotNull(service.getCredentialWrapper());
    assertTrue(service.getCredentialWrapper() instanceof DefaultCredentialWrapper);
    service = new GoogleCloudAccessTokenBuilder(new ConfiguredProduceDestination(), Arrays.asList("scope"));
    assertNotNull(service.getScope());
    assertTrue(service.getScope().contains("scope"));
    assertNotNull(service.getJsonKeyFile());
    assertTrue(service.getJsonKeyFile() instanceof ConfiguredProduceDestination);
  }

  @Test
  public void testInitFail() throws Exception {
    GoogleCloudAccessTokenBuilder service = new GoogleCloudAccessTokenBuilder();
    try {
      service.init();
      fail();
    } catch (CoreException e){
      assertEquals("Value for json-key-file is invalid", e.getMessage());
    }
  }

  @Test
  public void testInitJsonKeyOnly() throws Exception {
    GoogleCloudAccessTokenBuilder service = new GoogleCloudAccessTokenBuilder();
    service.setJsonKeyFile(new ConfiguredProduceDestination());
    try {
      service.init();
      fail();
    } catch (CoreException e){
      assertEquals("Value for scope is invalid", e.getMessage());
    }
  }

  @Test
  public void testInitScopeOnly() throws Exception {
    GoogleCloudAccessTokenBuilder service = new GoogleCloudAccessTokenBuilder();
    service.setScope(Arrays.asList("scope"));
    try {
      service.init();
      fail();
    } catch (CoreException e){
      assertEquals("Value for json-key-file is invalid", e.getMessage());
    }
  }

  @Test
  public void testInitOk() throws Exception {
    GoogleCloudAccessTokenBuilder service = new GoogleCloudAccessTokenBuilder();
    service.setScope(Arrays.asList("scope"));
    service.setJsonKeyFile(new ConfiguredProduceDestination());
    service.init();
    assertNotNull(service.getScope());
    assertTrue(service.getScope().contains("scope"));
    assertNotNull(service.getJsonKeyFile());
    assertTrue(service.getJsonKeyFile() instanceof ConfiguredProduceDestination);
  }

  @Test
  public void testBuild() throws Exception {
    AdaptrisMessage msg =  AdaptrisMessageFactory.getDefaultInstance().newMessage("Hello World");
    URL resource = GoogleCloudAccessTokenBuilderTest.class.getClassLoader().getResource("interlok.json");
    File jsonFile = Paths.get(resource.toURI()).toFile();
    GoogleCloudAccessTokenBuilder service = new GoogleCloudAccessTokenBuilder();
    service.setCredentialWrapper(new StubCredentialWrapper());
    service.setJsonKeyFile(new ConfiguredProduceDestination("file:///" + jsonFile.getAbsolutePath()));
    service.setScope(Arrays.asList("https://www.googleapis.com/auth/pubsub"));
    AccessToken accessToken = service.build(msg);
    assertEquals(accessToken.getToken(), StubCredentialWrapper.ACCESS_TOKEN);
    assertEquals(accessToken.getExpiry(), DateFormatUtil.format(StubCredentialWrapper.EXPIRATION));
  }

  @Override
  protected Object retrieveObjectForSampleConfig() {
    GetOauthToken service = new GetOauthToken();
    GoogleCloudAccessTokenBuilder tokenBuilder = new GoogleCloudAccessTokenBuilder();
    tokenBuilder.setScope(Arrays.asList("https://www.googleapis.com/auth/pubsub"));
    tokenBuilder.setJsonKeyFile(new ConfiguredProduceDestination("file:////home/matthew/interlok.json"));
    service.setAccessTokenBuilder(tokenBuilder);
    return service;
  }

  @Override
  protected String createBaseFileName(Object object) {
    return super.createBaseFileName(object) + "-GoogleCloudAccessTokenBuilder";
  }


}