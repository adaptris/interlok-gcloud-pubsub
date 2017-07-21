package com.adaptris.google.cloud.credential;

import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.AdaptrisMessageFactory;
import com.adaptris.core.ConfiguredProduceDestination;
import com.adaptris.core.CoreException;
import com.adaptris.core.http.oauth.AccessToken;
import com.adaptris.core.http.oauth.GetOauthToken;
import com.adaptris.core.util.LifecycleHelper;
import com.adaptris.util.text.DateFormatUtil;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Arrays;


public class GoogleCloudAccessTokenBuilderTest extends CredentialServiceExample {

  public GoogleCloudAccessTokenBuilderTest(String name) {
    super(name);
  }

  @Test
  public void testConstruct() throws Exception {
    GoogleCloudAccessTokenBuilder service = new GoogleCloudAccessTokenBuilder();
    assertNotNull(service.getCredentials());
    assertTrue(service.getCredentials() instanceof ApplicationDefaultCredentials);
    service = new GoogleCloudAccessTokenBuilder(new KeyFileCredentials());
    assertNotNull(service.getCredentials());
    assertTrue(service.getCredentials() instanceof KeyFileCredentials);
  }


  @Test
  public void testBuild() throws Exception {
    AdaptrisMessage msg =  AdaptrisMessageFactory.getDefaultInstance().newMessage("Hello World");
    GoogleCloudAccessTokenBuilder service = new GoogleCloudAccessTokenBuilder();
    Credentials credentials = Mockito.spy(new StubCredentialBuilder());
    service.setCredentials(credentials);
    LifecycleHelper.initAndStart(service);
    AccessToken accessToken = service.build(msg);
    LifecycleHelper.stopAndClose(service);
    assertEquals(accessToken.getToken(), StubCredentialBuilder.ACCESS_TOKEN);
    assertEquals(accessToken.getExpiry(), DateFormatUtil.format(StubCredentialBuilder.EXPIRATION));
    Mockito.verify(credentials, Mockito.times(1)).init();
    Mockito.verify(credentials, Mockito.times(1)).start();
    Mockito.verify(credentials, Mockito.times(1)).stop();
    Mockito.verify(credentials, Mockito.times(1)).close();
  }

  @Override
  protected Object retrieveObjectForSampleConfig() {
    GetOauthToken service = new GetOauthToken();
    ApplicationDefaultCredentials applicationDefaultCredentials = new ApplicationDefaultCredentials();
    applicationDefaultCredentials.setScopes(Arrays.asList("https://www.googleapis.com/auth/pubsub"));
    GoogleCloudAccessTokenBuilder tokenBuilder = new GoogleCloudAccessTokenBuilder(applicationDefaultCredentials);
    service.setAccessTokenBuilder(tokenBuilder);
    return service;
  }

  @Override
  protected String createBaseFileName(Object object) {
    return super.createBaseFileName(object) + "-GoogleCloudAccessTokenBuilder";
  }


}