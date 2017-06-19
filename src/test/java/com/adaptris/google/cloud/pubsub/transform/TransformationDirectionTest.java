package com.adaptris.google.cloud.pubsub.transform;

import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.AdaptrisMessageFactory;
import com.adaptris.core.metadata.NoOpMetadataFilter;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.ReadContext;
import com.jayway.jsonpath.spi.json.JsonSmartJsonProvider;
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider;
import org.junit.Before;
import org.junit.Test;

import java.util.EnumSet;

import static org.junit.Assert.*;

public class TransformationDirectionTest {


  private Configuration jsonConfig;

  @Before
  public void setUp() {
    jsonConfig = new Configuration.ConfigurationBuilder().jsonProvider(new JsonSmartJsonProvider())
        .mappingProvider(new JacksonMappingProvider()).options(EnumSet.noneOf(Option.class)).build();
  }

  @Test
  public void transform_INTERLOK_TO_PUBSUB() throws Exception {
    final AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage("Hello World");
    msg.addMetadata("foo", "bar");
    TransformationDirection.INTERLOK_TO_PUBSUB.transform(msg, new NoOpMetadataFilter());
    ReadContext context = JsonPath.parse(msg.getInputStream(), jsonConfig);
    assertNotNull(context.read("$.data"));
    assertEquals("SGVsbG8gV29ybGQ=", context.read("$.data"));
    assertNotNull(context.read("$.attributes"));
    assertNotNull(context.read("$.attributes.foo"));
    assertEquals("bar", context.read("$.attributes.foo"));
  }

  @Test
  public void transform_PUBSUB_TO_INTERLOK() throws Exception {
    final AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage(s);
    TransformationDirection.PUBSUB_TO_INTERLOK.transform(msg, new NoOpMetadataFilter());
    assertTrue(msg.headersContainsKey("iana.org/language_tag"));
    assertEquals("en", msg.getMetadataValue("iana.org/language_tag"));
    assertEquals("Hello Cloud Pub/Sub! Here is my message!", msg.getContent());
  }

  String s = "{\n" +
      "      \"attributes\": {\n" +
      "        \"iana.org/language_tag\": \"en\"\n" +
      "      },\n" +
      "      \"data\": \"SGVsbG8gQ2xvdWQgUHViL1N1YiEgSGVyZSBpcyBteSBtZXNzYWdlIQ==\"\n" +
      "    }";

}