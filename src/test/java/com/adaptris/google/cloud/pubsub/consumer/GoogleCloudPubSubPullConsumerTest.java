package com.adaptris.google.cloud.pubsub.consumer;

import com.adaptris.core.*;
import com.adaptris.core.stubs.MockMessageListener;
import com.adaptris.core.util.LifecycleHelper;
import com.adaptris.google.cloud.pubsub.connection.GoogleCloudPubSubConnection;
import com.adaptris.util.TimeInterval;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.SubscriptionName;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

public class GoogleCloudPubSubPullConsumerTest extends ConsumerCase {

  public GoogleCloudPubSubPullConsumerTest(String name) {
    super(name);
  }

  @Test
  public void testConstruct() throws Exception {
    GoogleCloudPubSubPullConsumer consumer = new GoogleCloudPubSubPullConsumer();
    assertNull(consumer.getDestination());
    assertNull(consumer.getSubscriptionName());
    assertNotNull(consumer.getAckDeadline());
    assertNotNull(consumer.getAckDeadlineSeconds());
    assertNotNull(consumer.getCreateSubscription());
    assertEquals(6000, consumer.getAckDeadline().toMilliseconds());
    assertEquals(6, consumer.getAckDeadlineSeconds());
    assertTrue(consumer.getCreateSubscription());
  }

  @Test
  public void testPrepare() throws Exception {
    GoogleCloudPubSubPullConsumer consumer = new GoogleCloudPubSubPullConsumer();
    prepareFail(consumer, "Subscription Name is invalid");
    consumer.setSubscriptionName("subscription");
    prepareFail(consumer, "Destination is invalid");
    consumer.setDestination(new ConfiguredConsumeDestination("topic"));
    consumer.setAckDeadline(null);
    prepareFail(consumer, "Ack Deadline is invalid");
    consumer.setAckDeadline(new TimeInterval(6L, TimeUnit.SECONDS));
    consumer.setCreateSubscription(null);
    prepareFail(consumer, "Create Subscription is invalid");
    consumer.setCreateSubscription(true);
    consumer.prepare();
  }

  private void prepareFail(GoogleCloudPubSubPullConsumer consumer, String message){
    try {
      consumer.prepare();
      fail();
    } catch (CoreException expected){
      assertEquals(message, expected.getMessage());
    }
  }

  @Test
  public void testTopic(){
    GoogleCloudPubSubPullConsumer consumer = new GoogleCloudPubSubPullConsumer();
    consumer.setDestination(new ConfiguredConsumeDestination("topic-name"));
    assertEquals("topic-name", consumer.getTopicName());
  }

  @Test
  public void testSubscriptionName(){
    GoogleCloudPubSubPullConsumer consumer = new GoogleCloudPubSubPullConsumer();
    consumer.setSubscriptionName("sub-name");
    assertEquals("sub-name", consumer.getSubscriptionName());
  }

  @Test
  public void testAckDeadline(){
    GoogleCloudPubSubPullConsumer consumer = new GoogleCloudPubSubPullConsumer();
    assertEquals(6000, consumer.getAckDeadline().toMilliseconds());
    assertEquals(6, consumer.getAckDeadlineSeconds());
    consumer.setAckDeadline(new TimeInterval(20L, TimeUnit.SECONDS));
    assertEquals(20, consumer.getAckDeadlineSeconds());
    assertEquals(20000, consumer.getAckDeadline().toMilliseconds());
  }

  @Test
  public void testCreateSubscription(){
    GoogleCloudPubSubPullConsumer consumer = new GoogleCloudPubSubPullConsumer();
    assertTrue(consumer.getCreateSubscription());
    consumer.setCreateSubscription(false);
    assertFalse(consumer.getCreateSubscription());
  }

  @Test
  public void testStart() throws Exception{
    GoogleCloudPubSubPullConsumer consumer = new GoogleCloudPubSubPullConsumer();
    consumer.setSubscriptionName("subscription-name");
    consumer.setDestination(new ConfiguredConsumeDestination("topic-name"));
    GoogleCloudPubSubConnection connection = Mockito.mock(GoogleCloudPubSubConnection.class);
    Mockito.doReturn("project-name").when(connection).getProjectName();
    SubscriptionName subscriptionName = Mockito.mock(SubscriptionName.class);
    Mockito.doReturn(subscriptionName).when(connection).createSubscription(consumer);
    Subscriber subscriber = Mockito.mock(Subscriber.class);
    Mockito.doReturn(subscriber).when(connection).createSubscriber(subscriptionName, consumer);
    StandaloneConsumer sc = new StandaloneConsumer(connection, consumer);
//    LifecycleHelper.init(sc);
//    LifecycleHelper.start(sc);

  }

  @Test
  public void testReceiveMessage() throws Exception {
    GoogleCloudPubSubPullConsumer consumer = new GoogleCloudPubSubPullConsumer();
    consumer.setDestination(new ConfiguredConsumeDestination("topic-name"));
    consumer.setSubscriptionName("subscription-name");
    consumer.setProjectName("project-name");
    MockMessageListener stub = new MockMessageListener();
    consumer.registerAdaptrisMessageListener(stub);
    ByteString byteString = ByteString.copyFrom("Hello World".getBytes());
    PubsubMessage psm1 = PubsubMessage.newBuilder()
        .setData(byteString)
        .setMessageId("123")
        .setPublishTime(Timestamp.newBuilder().setSeconds(1497951924L))
        .putAttributes("prop1", "value1")
        .putAttributes("prop2", "value2")
        .build();
    AckReplyConsumer ackReplyConsumer = Mockito.mock(AckReplyConsumer.class);

    consumer.receiveMessage(psm1, ackReplyConsumer);

    waitForMessages(stub, 1);
    assertEquals(1,stub.getMessages().size());
    AdaptrisMessage message1 = stub.getMessages().get(0);
    assertEquals("Hello World", message1.getContent());
    assertEquals("topic-name", message1.getMetadataValue("gcloud_topic"));
    assertEquals("project-name", message1.getMetadataValue("gcloud_projectName"));
    assertEquals("subscription-name", message1.getMetadataValue("gcloud_subscriptionName"));
    assertEquals("123", message1.getMetadataValue("gcloud_messageId"));
    assertEquals("1497951924", message1.getMetadataValue("gcloud_publishTime"));
    assertEquals("value1", message1.getMetadataValue("prop1"));
    assertEquals("value2", message1.getMetadataValue("prop2"));

    stub.getMessages().clear();

    PubsubMessage psm2 = PubsubMessage.newBuilder()
        .setData(byteString)
        .setMessageId("123")
        .putAttributes("prop1", "value1")
        .putAttributes("prop2", "value2").build();
    consumer.receiveMessage(psm2, ackReplyConsumer);
    assertEquals(1,stub.getMessages().size());
    AdaptrisMessage message2 = stub.getMessages().get(0);
    assertEquals("Hello World", message2.getContent());
    assertEquals("topic-name", message2.getMetadataValue("gcloud_topic"));
    assertEquals("project-name", message2.getMetadataValue("gcloud_projectName"));
    assertEquals("subscription-name", message2.getMetadataValue("gcloud_subscriptionName"));
    assertEquals("123", message1.getMetadataValue("gcloud_messageId"));
    assertFalse(message2.headersContainsKey("gcloud_publishTime"));
    assertEquals("value1", message2.getMetadataValue("prop1"));
    assertEquals("value2", message2.getMetadataValue("prop2"));
  }


  @Override
  protected Object retrieveObjectForSampleConfig() {
    GoogleCloudPubSubConnection conn = new GoogleCloudPubSubConnection();
    conn.setScopes(Arrays.asList("https://www.googleapis.com/auth/pubsub"));
    conn.setJsonKeyFile("file:////home/matthew/interlok.json");
    conn.setProjectName("project-name");

    GoogleCloudPubSubPullConsumer cons = new GoogleCloudPubSubPullConsumer();
    cons.setSubscriptionName("subscription-name");
    cons.setDestination(new ConfiguredConsumeDestination("topic-name"));

    StandaloneConsumer result = new StandaloneConsumer();
    result.setConnection(conn);
    result.setConsumer(cons);
    return result;
  }
}