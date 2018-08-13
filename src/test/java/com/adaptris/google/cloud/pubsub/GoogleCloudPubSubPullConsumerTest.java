package com.adaptris.google.cloud.pubsub;

import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.mockito.Mockito;

import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.ConfiguredConsumeDestination;
import com.adaptris.core.ConsumerCase;
import com.adaptris.core.CoreException;
import com.adaptris.core.StandaloneConsumer;
import com.adaptris.core.oauth.gcloud.ApplicationDefaultCredentials;
import com.adaptris.core.stubs.MockMessageListener;
import com.adaptris.core.util.LifecycleHelper;
import com.adaptris.google.cloud.pubsub.credentials.FixedCredentialsProvider;
import com.adaptris.util.TimeInterval;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PubsubMessage;

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
    assertEquals(10000, consumer.getAckDeadline().toMilliseconds());
    assertEquals(10, consumer.getAckDeadlineSeconds());
    assertTrue(consumer.getCreateSubscription());
    assertTrue(consumer.getAutoAcknowledge());
  }

  @Test
  public void testPrepare() throws Exception {
    GoogleCloudPubSubPullConsumer consumer = new GoogleCloudPubSubPullConsumer();
    prepareFail(consumer, "subscriptionName may not be null");
    consumer.setSubscriptionName("subscription");
    prepareFail(consumer, "destination may not be null");
    consumer.setDestination(new ConfiguredConsumeDestination("topic"));
    consumer.setAckDeadline(null);
    prepareFail(consumer, "ackDeadline may not be null");
    consumer.setAckDeadline(new TimeInterval(10L, TimeUnit.SECONDS));
    consumer.setCreateSubscription(null);
    prepareFail(consumer, "createSubscription may not be null");
    consumer.setCreateSubscription(true);
    consumer.setAutoAcknowledge(null);
    prepareFail(consumer, "autoAcknowledge may not be null");
    consumer.setAutoAcknowledge(true);
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
    assertEquals(10000, consumer.getAckDeadline().toMilliseconds());
    assertEquals(10, consumer.getAckDeadlineSeconds());
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
  public void testAutoAcknowledge(){
    GoogleCloudPubSubPullConsumer consumer = new GoogleCloudPubSubPullConsumer();
    assertTrue(consumer.getAutoAcknowledge());
    consumer.setAutoAcknowledge(false);
    assertFalse(consumer.getAutoAcknowledge());
  }

  @Test
  public void testLifecycle() throws Exception{
    GoogleCloudPubSubPullConsumer consumer = new GoogleCloudPubSubPullConsumer();
    consumer.setSubscriptionName("subscription-name");
    consumer.setDestination(new ConfiguredConsumeDestination("topic-name"));
    GoogleCloudPubSubConnection connection = Mockito.mock(GoogleCloudPubSubConnection.class);
    Mockito.doReturn(connection).when(connection).retrieveConnection(GoogleCloudPubSubConnection.class);
    Mockito.doReturn("project-name").when(connection).getProjectName();
    ProjectSubscriptionName subscription = ProjectSubscriptionName.newBuilder().setProject("project-name").setSubscription("subscription-name").build();
    Mockito.doReturn(subscription).when(connection).createSubscription(consumer);
    Subscriber subscriber = Mockito.mock(Subscriber.class);
    Mockito.doReturn(subscriber).when(subscriber).startAsync();
    Mockito.doReturn(subscriber).when(connection).createSubscriber(subscription, consumer);
    consumer.registerConnection(connection);
    LifecycleHelper.initAndStart(consumer);
    LifecycleHelper.stopAndClose(consumer);
    Mockito.verify(connection, Mockito.times(1)).createSubscription(consumer);
    Mockito.verify(connection, Mockito.times(1)).createSubscriber(subscription, consumer);
    Mockito.verify(connection, Mockito.times(1)).getProjectName();
    Mockito.verify(subscriber, Mockito.times(1)).startAsync();
    Mockito.verify(subscriber, Mockito.times(1)).awaitRunning();
    Mockito.verify(subscriber, Mockito.times(1)).stopAsync();
    Mockito.verify(connection, Mockito.times(1)).deleteSubscription(consumer);

  }

  @Test
  public void testStopWithNull() throws Exception{
    GoogleCloudPubSubPullConsumer consumer = new GoogleCloudPubSubPullConsumer();
    consumer.setSubscriptionName("subscription-name");
    consumer.setDestination(new ConfiguredConsumeDestination("topic-name"));
    GoogleCloudPubSubConnection connection = Mockito.mock(GoogleCloudPubSubConnection.class);
    Mockito.doReturn(connection).when(connection).retrieveConnection(GoogleCloudPubSubConnection.class);
    Mockito.doReturn("project-name").when(connection).getProjectName();
    ProjectSubscriptionName subscription = ProjectSubscriptionName.newBuilder().setProject("project-name").setSubscription("subscription-name").build();
    Mockito.doReturn(subscription).when(connection).createSubscription(consumer);
    Subscriber subscriber = Mockito.mock(Subscriber.class);
    Mockito.doReturn(subscriber).when(subscriber).startAsync();
    Mockito.doReturn(subscriber).when(connection).createSubscriber(subscription, consumer);
    consumer.stop();
    Mockito.verify(subscriber, Mockito.never()).stopAsync();
  }

  @Test
  public void testCloseException() throws Exception{
    GoogleCloudPubSubPullConsumer consumer = Mockito.spy(new GoogleCloudPubSubPullConsumer());
    consumer.setSubscriptionName("subscription-name");
    consumer.setDestination(new ConfiguredConsumeDestination("topic-name"));
    GoogleCloudPubSubConnection connection = Mockito.mock(GoogleCloudPubSubConnection.class);
    Mockito.doReturn(connection).when(connection).retrieveConnection(GoogleCloudPubSubConnection.class);
    Mockito.doReturn("project-name").when(connection).getProjectName();
    ProjectSubscriptionName subscription = ProjectSubscriptionName.newBuilder().setProject("project-name").setSubscription("subscription-name").build();
    Mockito.doReturn(subscription).when(connection).createSubscription(consumer);
    Subscriber subscriber = Mockito.mock(Subscriber.class);
    Mockito.doReturn(subscriber).when(subscriber).startAsync();
    Mockito.doReturn(subscriber).when(connection).createSubscriber(subscription, consumer);
    Mockito.doThrow(new CoreException()).when(connection).deleteSubscription(consumer);
    consumer.registerConnection(connection);
    consumer.close();
    Mockito.verify(connection, Mockito.times(1)).deleteSubscription(consumer);
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
    AckReplyConsumer ackReplyConsumer = Mockito.mock(AckReplyConsumer.class);
    PubsubMessage psm = PubsubMessage.newBuilder()
        .setData(byteString)
        .setMessageId("123")
        .putAttributes("prop1", "value1")
        .putAttributes("prop2", "value2").build();
    consumer.receiveMessage(psm, ackReplyConsumer);
    waitForMessages(stub, 1);
    assertEquals(1,stub.getMessages().size());
    AdaptrisMessage message = stub.getMessages().get(0);
    assertEquals("Hello World", message.getContent());
    assertEquals("topic-name", message.getMetadataValue("gcloud_topic"));
    assertEquals("project-name", message.getMetadataValue("gcloud_projectName"));
    assertEquals("subscription-name", message.getMetadataValue("gcloud_subscriptionName"));
    assertEquals("123", message.getMetadataValue("gcloud_messageId"));
    assertFalse(message.headersContainsKey("gcloud_publishTime"));
    assertEquals("value1", message.getMetadataValue("prop1"));
    assertEquals("value2", message.getMetadataValue("prop2"));
    Mockito.verify(ackReplyConsumer, Mockito.times(1)).ack();
  }

  @Test
  public void testReceiveMessagePublishTime() throws Exception {
    GoogleCloudPubSubPullConsumer consumer = new GoogleCloudPubSubPullConsumer();
    consumer.setDestination(new ConfiguredConsumeDestination("topic-name"));
    consumer.setSubscriptionName("subscription-name");
    consumer.setProjectName("project-name");
    MockMessageListener stub = new MockMessageListener();
    consumer.registerAdaptrisMessageListener(stub);
    ByteString byteString = ByteString.copyFrom("Hello World".getBytes());
    PubsubMessage psm = PubsubMessage.newBuilder()
        .setData(byteString)
        .setMessageId("123")
        .setPublishTime(Timestamp.newBuilder().setSeconds(1497951924L))
        .putAttributes("prop1", "value1")
        .putAttributes("prop2", "value2")
        .build();
    AckReplyConsumer ackReplyConsumer = Mockito.mock(AckReplyConsumer.class);

    consumer.receiveMessage(psm, ackReplyConsumer);

    waitForMessages(stub, 1);
    assertEquals(1,stub.getMessages().size());
    AdaptrisMessage message = stub.getMessages().get(0);
    assertEquals("Hello World", message.getContent());
    assertEquals("topic-name", message.getMetadataValue("gcloud_topic"));
    assertEquals("project-name", message.getMetadataValue("gcloud_projectName"));
    assertEquals("subscription-name", message.getMetadataValue("gcloud_subscriptionName"));
    assertEquals("123", message.getMetadataValue("gcloud_messageId"));
    assertEquals("1497951924", message.getMetadataValue("gcloud_publishTime"));
    assertEquals("value1", message.getMetadataValue("prop1"));
    assertEquals("value2", message.getMetadataValue("prop2"));
    Mockito.verify(ackReplyConsumer, Mockito.times(1)).ack();
  }

  @Test
  public void testReceiveMessageAutoAckFalse() throws Exception {
    GoogleCloudPubSubPullConsumer consumer = new GoogleCloudPubSubPullConsumer();
    consumer.setDestination(new ConfiguredConsumeDestination("topic-name"));
    consumer.setSubscriptionName("subscription-name");
    consumer.setProjectName("project-name");
    consumer.setAutoAcknowledge(false);
    MockMessageListener stub = new MockMessageListener();
    consumer.registerAdaptrisMessageListener(stub);
    ByteString byteString = ByteString.copyFrom("Hello World".getBytes());
    PubsubMessage psm = PubsubMessage.newBuilder()
        .setData(byteString)
        .setMessageId("123")
        .setPublishTime(Timestamp.newBuilder().setSeconds(1497951924L))
        .putAttributes("prop1", "value1")
        .putAttributes("prop2", "value2")
        .build();
    AckReplyConsumer ackReplyConsumer = Mockito.mock(AckReplyConsumer.class);

    consumer.receiveMessage(psm, ackReplyConsumer);

    waitForMessages(stub, 1);
    assertEquals(1,stub.getMessages().size());
    AdaptrisMessage message = stub.getMessages().get(0);
    assertEquals("Hello World", message.getContent());
    assertEquals("topic-name", message.getMetadataValue("gcloud_topic"));
    assertEquals("project-name", message.getMetadataValue("gcloud_projectName"));
    assertEquals("subscription-name", message.getMetadataValue("gcloud_subscriptionName"));
    assertEquals("123", message.getMetadataValue("gcloud_messageId"));
    assertEquals("1497951924", message.getMetadataValue("gcloud_publishTime"));
    assertEquals("value1", message.getMetadataValue("prop1"));
    assertEquals("value2", message.getMetadataValue("prop2"));
    assertTrue(message.getObjectHeaders().containsKey(Constants.REPLY_KEY));
    Object obj = message.getObjectHeaders().get(Constants.REPLY_KEY);
    assertTrue(obj instanceof AckReplyConsumer);
    assertEquals(ackReplyConsumer, obj);
    Mockito.verify(ackReplyConsumer, Mockito.never()).ack();
  }


  @Override
  protected Object retrieveObjectForSampleConfig() {
    GoogleCloudPubSubConnection conn = new GoogleCloudPubSubConnection();
    conn.setProjectName("project-name");
    conn.setCredentialsProvider(new FixedCredentialsProvider(new ApplicationDefaultCredentials("https://www.googleapis.com/auth/pubsub")));

    GoogleCloudPubSubPullConsumer cons = new GoogleCloudPubSubPullConsumer();
    cons.setSubscriptionName("subscription-name");
    cons.setDestination(new ConfiguredConsumeDestination("topic-name"));

    StandaloneConsumer result = new StandaloneConsumer();
    result.setConnection(conn);
    result.setConsumer(cons);
    return result;
  }
}