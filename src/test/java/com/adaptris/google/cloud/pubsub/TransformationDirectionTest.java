package com.adaptris.google.cloud.pubsub;

import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.AdaptrisMessageFactory;
import com.adaptris.core.ServiceException;
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
  public void transform_INTERLOK_TO_PUBLISH_REQUEST() throws Exception {
    final AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage("Hello World");
    msg.addMetadata("foo", "bar");
    TransformationDirection.INTERLOK_TO_PUBLISH_REQUEST.transform(msg, new NoOpMetadataFilter());
    ReadContext context = JsonPath.parse(msg.getInputStream(), jsonConfig);
    assertNotNull(context.read("$.messages.[0].data"));
    assertEquals("SGVsbG8gV29ybGQ=", context.read("$.messages.[0].data"));
    assertNotNull(context.read("$.messages.[0].attributes"));
    assertNotNull(context.read("$.messages.[0].attributes.foo"));
    assertEquals("bar", context.read("$.messages.[0].attributes.foo"));
  }

  @Test
  public void transform_PULL_RESPONSE_TO_INTERLOK() throws Exception {
    final AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage(pullResponse);
    TransformationDirection.PULL_RESPONSE_TO_INTERLOK.transform(msg, new NoOpMetadataFilter());
    assertTrue(msg.headersContainsKey("fsConsumeDir"));
    assertEquals("/opt/interlok/messages/in/", msg.getMetadataValue("fsConsumeDir"));
    assertEquals("Hello Cloud Pub/Sub! Here is my message!", msg.getContent());
    assertTrue(msg.headersContainsKey("gcloud_ackId"));
    assertEquals("projects/interlok-test/subscriptions/mysubscription:1", msg.getMetadataValue("gcloud_ackId"));
    assertTrue(msg.headersContainsKey("gcloud_messageId"));
    assertEquals("2", msg.getMetadataValue("gcloud_messageId"));
    assertTrue(msg.headersContainsKey("gcloud_publishTimeSeconds"));
    assertEquals("1497951924", msg.getMetadataValue("gcloud_publishTimeSeconds"));
  }

  @Test(expected = ServiceException.class)
  public void transform_PULL_RESPONSE_TO_INTERLOK_Exception() throws Exception {
    final AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage(pullResponseInvalid);
    TransformationDirection.PULL_RESPONSE_TO_INTERLOK.transform(msg, new NoOpMetadataFilter());
  }

  @Test
  public void transform_PUSH_RESPONSE_TO_INTERLOK() throws Exception {
    final AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage(pushResponse);
    TransformationDirection.PUSH_RESPONSE_TO_INTERLOK.transform(msg, new NoOpMetadataFilter());
    assertTrue(msg.headersContainsKey("fsConsumeDir"));
    assertEquals("/opt/interlok/messages/in/", msg.getMetadataValue("fsConsumeDir"));
    assertEquals("Hello Cloud Pub/Sub! Here is my message!", msg.getContent());
    assertTrue(msg.headersContainsKey("gcloud_messageId"));
    assertEquals("3", msg.getMetadataValue("gcloud_messageId"));
    assertFalse(msg.headersContainsKey("gcloud_ackId"));
    assertFalse(msg.headersContainsKey("gcloud_publishTime"));
  }

  String pullResponse = "{\n" +
      "    \"receivedMessages\": [\n" +
      "        {\n" +
      "            \"ackId\": \"projects/interlok-test/subscriptions/mysubscription:1\",\n" +
      "            \"message\": {\n" +
      "                \"attributes\": {\n" +
      "                    \"fsConsumeDir\": \"/opt/interlok/messages/in/\",\n" +
      "                    \"fsParentDir\": \"in\",\n" +
      "                    \"originalname\": \"text.tst\",\n" +
      "                    \"lastmodified\": \"1497951921000\",\n" +
      "                    \"fsFileSize\": \"0\",\n" +
      "                    \"adpnextmlemarkersequence\": \"1\"\n" +
      "                },\n" +
      "                \"data\": \"SGVsbG8gQ2xvdWQgUHViL1N1YiEgSGVyZSBpcyBteSBtZXNzYWdlIQ==\",\n" +
      "                \"messageId\": \"2\",\n" +
      "                \"publishTime\": \"2017-06-20T09:45:24Z\"\n" +
      "            }\n" +
      "        }\n" +
      "    ]\n" +
      "}";

  String pullResponseInvalid = "{\n" +
      "    \"receivedMessages\": [\n" +
      "        {\n" +
      "            \"ackId\": \"projects/interlok-test/subscriptions/mysubscription:1\",\n" +
      "            \"message\": {\n" +
      "                \"attributes\": {\n" +
      "                    \"fsConsumeDir\": \"/opt/interlok/messages/in/\",\n" +
      "                    \"fsParentDir\": \"in\",\n" +
      "                    \"originalname\": \"text.tst\",\n" +
      "                    \"lastmodified\": \"1497951921000\",\n" +
      "                    \"fsFileSize\": \"0\",\n" +
      "                    \"adpnextmlemarkersequence\": \"1\"\n" +
      "                },\n" +
      "                \"data\": \"SGVsbG8gQ2xvdWQgUHViL1N1YiEgSGVyZSBpcyBteSBtZXNzYWdlIQ==\",\n" +
      "                \"messageId\": \"2\",\n" +
      "                \"publishTime\": \"2017-06-20T09:45:24Z\"\n" +
      "            }\n" +
      "        },\n" +
      "        {\n" +
      "            \"ackId\": \"projects/interlok-test/subscriptions/mysubscription:1\",\n" +
      "            \"message\": {\n" +
      "                \"attributes\": {\n" +
      "                    \"fsConsumeDir\": \"/opt/interlok/messages/in/\",\n" +
      "                    \"fsParentDir\": \"in\",\n" +
      "                    \"originalname\": \"text.tst\",\n" +
      "                    \"lastmodified\": \"1497951921000\",\n" +
      "                    \"fsFileSize\": \"0\",\n" +
      "                    \"adpnextmlemarkersequence\": \"1\"\n" +
      "                },\n" +
      "                \"data\": \"SGVsbG8gQ2xvdWQgUHViL1N1YiEgSGVyZSBpcyBteSBtZXNzYWdlIQ==\",\n" +
      "                \"messageId\": \"2\",\n" +
      "                \"publishTime\": \"2017-06-20T09:45:24Z\"\n" +
      "            }\n" +
      "        }\n" +
      "    ]\n" +
      "}";

  String pushResponse = "{\n" +
      "  \"message\": {\n" +
      "    \"data\": \"SGVsbG8gQ2xvdWQgUHViL1N1YiEgSGVyZSBpcyBteSBtZXNzYWdlIQ==\",\n" +
      "    \"attributes\": {\n" +
      "      \"adpnextmlemarkersequence\": \"1\",\n" +
      "      \"fsConsumeDir\": \"/opt\\/interlok/messages/in/\",\n" +
      "      \"fsFileSize\": \"0\",\n" +
      "      \"lastmodified\": \"1497960722000\",\n" +
      "      \"fsParentDir\": \"in\",\n" +
      "      \"originalname\": \"text.tst\"\n" +
      "    },\n" +
      "    \"messageId\": \"3\"\n" +
      "  },\n" +
      "  \"subscription\": \"projects/interlok-test/subscriptions/mysubscription\"\n" +
      "}";
}