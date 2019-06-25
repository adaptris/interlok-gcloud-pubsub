/*
 * Copyright 2017, Google Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/*
 * Code based on https://github.com/GoogleCloudPlatform/google-cloud-java/tree/master/google-cloud-pubsub/src/test/java/com/google/cloud/pubsub/v1
 */
package com.adaptris.google.cloud.pubsub;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import java.util.List;
import org.junit.Test;
import org.mockito.Mockito;
import com.adaptris.core.ConfiguredConsumeDestination;
import com.adaptris.core.CoreException;
import com.adaptris.google.cloud.pubsub.adminclient.SubscriptionAdminClientProvider;
import com.adaptris.google.cloud.pubsub.channel.MockChannelProvider;
import com.adaptris.google.cloud.pubsub.credentials.MockCredentialsProvider;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.protobuf.AbstractMessage;
import com.google.protobuf.Empty;
import com.google.pubsub.v1.DeleteSubscriptionRequest;
import com.google.pubsub.v1.GetSubscriptionRequest;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PushConfig;
import com.google.pubsub.v1.Subscription;
import com.google.pubsub.v1.TopicName;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

public class PubsubConnectionTest extends ServiceHelperBase {

  @Test
  @SuppressWarnings("all")
  public void testCreateSubscriptionExists() throws Exception {
    ProjectSubscriptionName name = ProjectSubscriptionName.of(PROJECT, SUBSCRIPTION);
    TopicName topic = ProjectTopicName.of(PROJECT, TOPIC);
    int ackDeadlineSeconds2 = -921632575;
    boolean retainAckedMessages = false;
    Subscription expectedResponse =
        Subscription.newBuilder()
            .setName(name.toString())
            .setTopic(topic.toString())
            .setAckDeadlineSeconds(ackDeadlineSeconds2)
            .setRetainAckedMessages(retainAckedMessages)
            .build();
    mockSubscriber.addResponse(expectedResponse);

    GoogleCloudPubSubConnection connection = new GoogleCloudPubSubConnection();
    SubscriptionAdminClientProvider provider = Mockito.mock(SubscriptionAdminClientProvider.class);
    Mockito.doReturn(subscriptionAdminClient).when(provider).getSubscriptionAdminClient();
    connection.setSubscriptionAdminClientProvider(provider);
    connection.setProjectName(PROJECT);
    GoogleCloudPubSubPullConsumer consumer = new GoogleCloudPubSubPullConsumer();
    consumer.setDestination(new ConfiguredConsumeDestination(TOPIC));
    consumer.setSubscriptionName(SUBSCRIPTION);
    consumer.setCreateSubscription(true);
    ProjectSubscriptionName actualResponse = connection.createSubscription(consumer);
    assertEquals(ProjectSubscriptionName.parse(expectedResponse.getName()), actualResponse);

    List<AbstractMessage> actualRequests = mockSubscriber.getRequests();
    assertEquals(1, actualRequests.size());
    GetSubscriptionRequest actualRequest = (GetSubscriptionRequest) actualRequests.get(0);

    assertEquals(String.format("projects/%s/subscriptions/%s",connection.getProjectName(), consumer.getSubscriptionName()), actualRequest.getSubscription());
  }

  @Test
  @SuppressWarnings("all")
  public void testCreateSubscriptionDifferentTopic() throws Exception {
    ProjectSubscriptionName name = ProjectSubscriptionName.of(PROJECT, SUBSCRIPTION);
    TopicName topic = ProjectTopicName.of(PROJECT, TOPIC);
    int ackDeadlineSeconds2 = -921632575;
    boolean retainAckedMessages = false;
    Subscription expectedResponse =
        Subscription.newBuilder()
            .setName(name.toString())
            .setTopic(topic.toString())
            .setAckDeadlineSeconds(ackDeadlineSeconds2)
            .setRetainAckedMessages(retainAckedMessages)
            .build();
    mockSubscriber.addResponse(expectedResponse);

    GoogleCloudPubSubConnection connection = new GoogleCloudPubSubConnection();
    SubscriptionAdminClientProvider provider = Mockito.mock(SubscriptionAdminClientProvider.class);
    Mockito.doReturn(subscriptionAdminClient).when(provider).getSubscriptionAdminClient();
    connection.setSubscriptionAdminClientProvider(provider);
    connection.setProjectName(PROJECT);
    GoogleCloudPubSubPullConsumer consumer = new GoogleCloudPubSubPullConsumer();
    consumer.setDestination(new ConfiguredConsumeDestination("different-topic"));
    consumer.setSubscriptionName(SUBSCRIPTION);
    consumer.setCreateSubscription(true);
    try {
      connection.createSubscription(consumer);
      fail("No exception raised");
    } catch (CoreException e){
      assertTrue(e.getMessage().contains("Existing subscription topics do not match"));
    }
  }

  @Test
  @SuppressWarnings("all")
  public void testCreateSubscriptionException() throws Exception {
    StatusRuntimeException exception = new StatusRuntimeException(Status.PERMISSION_DENIED);
    mockSubscriber.addException(exception);

    GoogleCloudPubSubConnection connection = new GoogleCloudPubSubConnection();
    SubscriptionAdminClientProvider provider = Mockito.mock(SubscriptionAdminClientProvider.class);
    Mockito.doReturn(subscriptionAdminClient).when(provider).getSubscriptionAdminClient();
    connection.setSubscriptionAdminClientProvider(provider);
    connection.setProjectName(PROJECT);
    GoogleCloudPubSubPullConsumer consumer = new GoogleCloudPubSubPullConsumer();
    consumer.setDestination(new ConfiguredConsumeDestination("different-topic"));
    consumer.setSubscriptionName(SUBSCRIPTION);
    consumer.setCreateSubscription(true);
    try {
      connection.createSubscription(consumer);
      fail("No exception raised");
    } catch (CoreException e){
      assertEquals("Failed to retrieve Topic", e.getMessage());
    }
  }

  @Test
  @SuppressWarnings("all")
  public void testCreateSubscriptionExceptionNoCreate() throws Exception {
    StatusRuntimeException exception = new StatusRuntimeException(Status.NOT_FOUND);
    mockSubscriber.addException(exception);

    GoogleCloudPubSubConnection connection = new GoogleCloudPubSubConnection();
    SubscriptionAdminClientProvider provider = Mockito.mock(SubscriptionAdminClientProvider.class);
    Mockito.doReturn(subscriptionAdminClient).when(provider).getSubscriptionAdminClient();
    connection.setSubscriptionAdminClientProvider(provider);
    connection.setProjectName(PROJECT);
    GoogleCloudPubSubPullConsumer consumer = new GoogleCloudPubSubPullConsumer();
    consumer.setDestination(new ConfiguredConsumeDestination("different-topic"));
    consumer.setSubscriptionName(SUBSCRIPTION);
    consumer.setCreateSubscription(false);
    try {
      connection.createSubscription(consumer);
      fail("No exception raised");
    } catch (CoreException e){
      assertEquals("Failed to retrieve Topic", e.getMessage());
    }
  }

  @Test
  @SuppressWarnings("all")
  public void testCreateSubscriptionGetFailsThenCreate() throws Exception {
    StatusRuntimeException exception = new StatusRuntimeException(Status.NOT_FOUND);
    mockSubscriber.addException(exception);
    ProjectSubscriptionName name = ProjectSubscriptionName.of(PROJECT, SUBSCRIPTION);
    TopicName topic = ProjectTopicName.of(PROJECT, TOPIC);
    int ackDeadlineSeconds2 = -921632575;
    boolean retainAckedMessages = false;
    Subscription expectedResponse =
        Subscription.newBuilder()
            .setName(name.toString())
            .setTopic(topic.toString())
            .setAckDeadlineSeconds(ackDeadlineSeconds2)
            .setRetainAckedMessages(retainAckedMessages)
            .build();
    mockSubscriber.addResponse(expectedResponse);

    GoogleCloudPubSubConnection connection = new GoogleCloudPubSubConnection();
    SubscriptionAdminClientProvider provider = Mockito.mock(SubscriptionAdminClientProvider.class);
    Mockito.doReturn(subscriptionAdminClient).when(provider).getSubscriptionAdminClient();
    connection.setSubscriptionAdminClientProvider(provider);
    connection.setProjectName(PROJECT);
    GoogleCloudPubSubPullConsumer consumer = new GoogleCloudPubSubPullConsumer();
    consumer.setDestination(new ConfiguredConsumeDestination(TOPIC));
    consumer.setSubscriptionName(SUBSCRIPTION);
    consumer.setCreateSubscription(true);
    ProjectSubscriptionName actualResponse = connection.createSubscription(consumer);
    assertEquals(ProjectSubscriptionName.parse(expectedResponse.getName()), actualResponse);

    List<AbstractMessage> actualRequests = mockSubscriber.getRequests();
    assertEquals(1, actualRequests.size());
    Subscription actualRequest = (Subscription) actualRequests.get(0);

    assertEquals(String.format("projects/%s/subscriptions/%s", connection.getProjectName(), consumer.getSubscriptionName()), actualRequest.getName());
    assertEquals(String.format("projects/%s/topics/%s", connection.getProjectName(), consumer.getTopicName()), actualRequest.getTopic());
    assertEquals(PushConfig.getDefaultInstance(), actualRequest.getPushConfig());
    assertEquals(consumer.getAckDeadlineSeconds(), actualRequest.getAckDeadlineSeconds());
  }


  @Test
  @SuppressWarnings("all")
  public void testDeleteSubscription() throws Exception {
    Empty expectedResponse = Empty.newBuilder().build();
    mockSubscriber.addResponse(expectedResponse);

    GoogleCloudPubSubConnection connection = new GoogleCloudPubSubConnection();
    SubscriptionAdminClientProvider provider = Mockito.mock(SubscriptionAdminClientProvider.class);
    Mockito.doReturn(subscriptionAdminClient).when(provider).getSubscriptionAdminClient();
    connection.setSubscriptionAdminClientProvider(provider);
    connection.setProjectName(PROJECT);
    GoogleCloudPubSubPullConsumer consumer = new GoogleCloudPubSubPullConsumer();
    consumer.setDestination(new ConfiguredConsumeDestination(TOPIC));
    consumer.setSubscriptionName(SUBSCRIPTION);
    consumer.setCreateSubscription(true);
    connection.deleteSubscription(consumer);

    ProjectSubscriptionName subscription = ProjectSubscriptionName.of(PROJECT, SUBSCRIPTION);
    List<AbstractMessage> actualRequests = mockSubscriber.getRequests();
    assertEquals(1, actualRequests.size());
    DeleteSubscriptionRequest actualRequest = (DeleteSubscriptionRequest) actualRequests.get(0);
    assertEquals(subscription, ProjectSubscriptionName.parse(actualRequest.getSubscription()));
  }

  @Test
  @SuppressWarnings("all")
  public void testDeleteSubscriptionNoCreate() throws Exception {
    Empty expectedResponse = Empty.newBuilder().build();
    mockSubscriber.addResponse(expectedResponse);

    GoogleCloudPubSubConnection connection = new GoogleCloudPubSubConnection();
    SubscriptionAdminClientProvider provider = Mockito.mock(SubscriptionAdminClientProvider.class);
    Mockito.doReturn(subscriptionAdminClient).when(provider).getSubscriptionAdminClient();
    connection.setSubscriptionAdminClientProvider(provider);
    connection.setProjectName(PROJECT);
    GoogleCloudPubSubPullConsumer consumer = new GoogleCloudPubSubPullConsumer();
    consumer.setDestination(new ConfiguredConsumeDestination(TOPIC));
    consumer.setSubscriptionName(SUBSCRIPTION);
    consumer.setCreateSubscription(false);
    connection.deleteSubscription(consumer);
    List<AbstractMessage> actualRequests = mockSubscriber.getRequests();
    assertEquals(0, actualRequests.size());
  }

  @Test
  @SuppressWarnings("all")
  public void testCreateSubscriber() throws Exception {
    ProjectSubscriptionName name = ProjectSubscriptionName.of(PROJECT, SUBSCRIPTION);
    TopicName topic = ProjectTopicName.of(PROJECT, TOPIC);
    int ackDeadlineSeconds2 = -921632575;
    boolean retainAckedMessages = false;
    Subscription expectedResponse =
        Subscription.newBuilder()
            .setName(name.toString())
            .setTopic(topic.toString())
            .setAckDeadlineSeconds(ackDeadlineSeconds2)
            .setRetainAckedMessages(retainAckedMessages)
            .build();
    mockSubscriber.addResponse(expectedResponse);

    GoogleCloudPubSubConnection connection = new GoogleCloudPubSubConnection();
    SubscriptionAdminClientProvider provider = Mockito.mock(SubscriptionAdminClientProvider.class);
    Mockito.doReturn(subscriptionAdminClient).when(provider).getSubscriptionAdminClient();
    connection.setSubscriptionAdminClientProvider(provider);
    connection.setProjectName(PROJECT);
    connection.setCredentialsProvider(new MockCredentialsProvider(credentialsProvider));
    connection.setChannelProvider(new MockChannelProvider(channelProvider));

    GoogleCloudPubSubPullConsumer consumer = new GoogleCloudPubSubPullConsumer();
    consumer.setDestination(new ConfiguredConsumeDestination(TOPIC));
    consumer.setSubscriptionName(SUBSCRIPTION);
    consumer.setCreateSubscription(true);

    connection.initConnection();
    connection.startConnection();

    ProjectSubscriptionName subscription = connection.createSubscription(consumer);
    Subscriber subscriber = connection.createSubscriber(subscription, consumer);

    assertEquals(PROJECT, ProjectSubscriptionName.parse(subscriber.getSubscriptionNameString()).getProject());
    assertEquals(SUBSCRIPTION, ProjectSubscriptionName.parse(subscriber.getSubscriptionNameString()).getSubscription());

    List<AbstractMessage> actualRequests = mockSubscriber.getRequests();
    assertEquals(1, actualRequests.size());
  }

  @Test
  @SuppressWarnings("all")
  public void testCreateSubscriberWithErrorHandler() throws Exception {
    ProjectSubscriptionName name = ProjectSubscriptionName.of(PROJECT, SUBSCRIPTION);
    TopicName topic = ProjectTopicName.of(PROJECT, TOPIC);
    int ackDeadlineSeconds2 = -921632575;
    boolean retainAckedMessages = false;
    Subscription expectedResponse =
        Subscription.newBuilder()
            .setName(name.toString())
            .setTopic(topic.toString())
            .setAckDeadlineSeconds(ackDeadlineSeconds2)
            .setRetainAckedMessages(retainAckedMessages)
            .build();
    mockSubscriber.addResponse(expectedResponse);

    GoogleCloudPubSubConnection connection = new GoogleCloudPubSubConnection();
    SubscriptionAdminClientProvider provider = Mockito.mock(SubscriptionAdminClientProvider.class);
    Mockito.doReturn(subscriptionAdminClient).when(provider).getSubscriptionAdminClient();
    connection.setSubscriptionAdminClientProvider(provider);
    connection.setProjectName(PROJECT);
    connection.setCredentialsProvider(new MockCredentialsProvider(credentialsProvider));
    connection.setChannelProvider(new MockChannelProvider(channelProvider));
    connection.setConnectionErrorHandler(new GoogleCloudPubSubConnectionErrorHandler());

    GoogleCloudPubSubPullConsumer consumer = new GoogleCloudPubSubPullConsumer();
    consumer.setDestination(new ConfiguredConsumeDestination(TOPIC));
    consumer.setSubscriptionName(SUBSCRIPTION);
    consumer.setCreateSubscription(true);

    connection.initConnection();
    connection.startConnection();

    ProjectSubscriptionName subscription = connection.createSubscription(consumer);
    Subscriber subscriber = connection.createSubscriber(subscription, consumer);

    assertEquals(PROJECT, ProjectSubscriptionName.parse(subscriber.getSubscriptionNameString()).getProject());
    assertEquals(SUBSCRIPTION, ProjectSubscriptionName.parse(subscriber.getSubscriptionNameString()).getSubscription());
    //check for listener

    List<AbstractMessage> actualRequests = mockSubscriber.getRequests();
    assertEquals(1, actualRequests.size());
  }


}
