package com.adaptris.google.cloud.pubsub;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.EnumSet;

import org.junit.Before;
import org.junit.Test;

import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.AdaptrisMessageFactory;
import com.adaptris.core.metadata.NoOpMetadataFilter;
import com.adaptris.interlok.junit.scaffolding.services.TransformServiceExample;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.ReadContext;
import com.jayway.jsonpath.spi.json.JsonSmartJsonProvider;
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider;

public class GoogleCloudPubSubTransformServiceTest extends TransformServiceExample {

  private Configuration jsonConfig;

  @Before
  public void setUp() {
    jsonConfig = new Configuration.ConfigurationBuilder().jsonProvider(new JsonSmartJsonProvider())
        .mappingProvider(new JacksonMappingProvider()).options(EnumSet.noneOf(Option.class)).build();
  }

  @Test
  public void testConstruct(){
    GoogleCloudPubSubTransformService service = new GoogleCloudPubSubTransformService();
    assertNotNull(service.getDirection());
    assertEquals(TransformationDirection.INTERLOK_TO_PUBLISH_REQUEST, service.getDirection());
    assertNotNull(service.getDriver());
    assertTrue(service.getDriver() instanceof DefaultGoogleCloudPubSubTransformationDriver);
    assertNotNull(service.getMetadataFilter());
    assertTrue(service.getMetadataFilter() instanceof  NoOpMetadataFilter);
    service = new GoogleCloudPubSubTransformService(TransformationDirection.PULL_RESPONSE_TO_INTERLOK);
    assertNotNull(service.getDirection());
    assertEquals(TransformationDirection.PULL_RESPONSE_TO_INTERLOK, service.getDirection());
    assertNotNull(service.getDriver());
    assertTrue(service.getDriver() instanceof  DefaultGoogleCloudPubSubTransformationDriver);
    assertNotNull(service.getMetadataFilter());
    assertTrue(service.getMetadataFilter() instanceof  NoOpMetadataFilter);
    service = new GoogleCloudPubSubTransformService(TransformationDirection.PULL_RESPONSE_TO_INTERLOK, new DefaultGoogleCloudPubSubTransformationDriver());
    assertNotNull(service.getDirection());
    assertEquals(TransformationDirection.PULL_RESPONSE_TO_INTERLOK, service.getDirection());
    assertNotNull(service.getDriver());
    assertTrue(service.getDriver() instanceof  DefaultGoogleCloudPubSubTransformationDriver);
    assertNotNull(service.getMetadataFilter());
    assertTrue(service.getMetadataFilter() instanceof  NoOpMetadataFilter);
  }

  @Test
  public void testDefaultServiceExecute() throws Exception {
    AdaptrisMessage msg =  AdaptrisMessageFactory.getDefaultInstance().newMessage("Hello World");
    msg.addMetadata("foo", "bar");
    execute(new GoogleCloudPubSubTransformService(),msg);
    ReadContext context = JsonPath.parse(msg.getInputStream(), jsonConfig);
    assertNotNull(context.read("$.messages.[0].data"));
    assertEquals("SGVsbG8gV29ybGQ=", context.read("$.messages.[0].data"));
    assertNotNull(context.read("$.messages.[0].attributes"));
    assertNotNull(context.read("$.messages.[0].attributes.foo"));
    assertEquals("bar", context.read("$.messages.[0].attributes.foo"));
  }

  @Override
  protected Object retrieveObjectForSampleConfig() {
    return new GoogleCloudPubSubTransformService();
  }

}
