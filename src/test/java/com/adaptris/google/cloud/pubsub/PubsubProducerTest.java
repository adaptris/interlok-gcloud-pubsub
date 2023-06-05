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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import java.util.List;

import org.junit.jupiter.api.Test;

import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.AdaptrisMessageFactory;
import com.adaptris.core.CoreException;
import com.adaptris.core.ProduceException;
import com.adaptris.google.cloud.pubsub.adminclient.TopicAdminClientProvider;
import com.adaptris.google.cloud.pubsub.channel.MockChannelProvider;
import com.adaptris.google.cloud.pubsub.credentials.MockCredentialsProvider;
import com.google.protobuf.AbstractMessage;
import com.google.protobuf.Empty;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PublishRequest;
import com.google.pubsub.v1.PublishResponse;
import com.google.pubsub.v1.Topic;
import com.google.pubsub.v1.TopicName;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;

public class PubsubProducerTest extends ServiceHelperBase {

  @Test
  @SuppressWarnings("all")
  public void testCreateOrGetTopicName_GetNoCheck() throws Exception {
    Empty expectedResponse = Empty.newBuilder().build();
    mockPublisher.addResponse(expectedResponse);

    GoogleCloudPubSubConnection connection = new GoogleCloudPubSubConnection();
    TopicAdminClientProvider provider = mock(TopicAdminClientProvider.class);
    doReturn(topicAdminClient).when(provider).getTopicAdminClient();
    connection.setTopicAdminClientProvider(provider);
    connection.setProjectName(PROJECT);
    GoogleCloudPubSubProducer producer = new GoogleCloudPubSubProducer();
    producer.setTopic(TOPIC);
    producer.registerConnection(connection);
    producer.setCreateTopic(false);

    final AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage();
    producer.init();
    producer.start();
    TopicName topicName = producer.createOrGetTopicName(msg, producer.endpoint(msg));
    producer.stop();
    producer.close();

    List<AbstractMessage> actualRequests = mockPublisher.getRequests();
    assertEquals(0, actualRequests.size());

    assertEquals(TOPIC, topicName.getTopic());
    assertEquals(PROJECT, topicName.getProject());
  }

  @Test
  @SuppressWarnings("all")
  public void testCreateOrGetTopicName_GetWithCreate() throws Exception {
    Topic expectedResponse = Topic.newBuilder().setName(ProjectTopicName.of(PROJECT, TOPIC).toString()).build();
    mockPublisher.addResponse(expectedResponse);

    GoogleCloudPubSubConnection connection = new GoogleCloudPubSubConnection();
    TopicAdminClientProvider provider = mock(TopicAdminClientProvider.class);
    doReturn(topicAdminClient).when(provider).getTopicAdminClient();
    connection.setTopicAdminClientProvider(provider);
    connection.setProjectName(PROJECT);
    GoogleCloudPubSubProducer producer = new GoogleCloudPubSubProducer();
    producer.setTopic(TOPIC);
    producer.registerConnection(connection);
    producer.setCreateTopic(true);

    final AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage();
    producer.init();
    producer.start();
    TopicName topicName = producer.createOrGetTopicName(msg, producer.endpoint(msg));
    producer.stop();
    producer.close();

    List<AbstractMessage> actualRequests = mockPublisher.getRequests();
    assertEquals(1, actualRequests.size());

    assertEquals(TOPIC, topicName.getTopic());
    assertEquals(PROJECT, topicName.getProject());
  }

  @Test
  @SuppressWarnings("all")
  public void testCreateOrGetTopicName_GetWithCreate_NotFound() throws Exception {
    StatusRuntimeException exception = new StatusRuntimeException(Status.NOT_FOUND);
    mockPublisher.addException(exception);
    Topic expectedResponse = Topic.newBuilder().setName(ProjectTopicName.of(PROJECT, TOPIC).toString()).build();
    mockPublisher.addResponse(expectedResponse);

    GoogleCloudPubSubConnection connection = new GoogleCloudPubSubConnection();
    TopicAdminClientProvider provider = mock(TopicAdminClientProvider.class);
    doReturn(topicAdminClient).when(provider).getTopicAdminClient();
    connection.setTopicAdminClientProvider(provider);
    connection.setProjectName(PROJECT);
    GoogleCloudPubSubProducer producer = new GoogleCloudPubSubProducer();
    producer.setTopic(TOPIC);
    producer.registerConnection(connection);
    producer.setCreateTopic(true);

    final AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage();
    producer.init();
    producer.start();
    TopicName topicName = producer.createOrGetTopicName(msg, producer.endpoint(msg));
    producer.stop();
    producer.close();

    List<AbstractMessage> actualRequests = mockPublisher.getRequests();
    assertEquals(1, actualRequests.size());

    assertEquals(TOPIC, topicName.getTopic());
    assertEquals(PROJECT, topicName.getProject());
  }

  @Test
  @SuppressWarnings("all")
  public void testCreateOrGetTopicName_GetWithCreate_Exception() throws Exception {
    StatusRuntimeException exception = new StatusRuntimeException(Status.UNAUTHENTICATED);
    mockPublisher.addException(exception);

    GoogleCloudPubSubConnection connection = new GoogleCloudPubSubConnection();
    TopicAdminClientProvider provider = mock(TopicAdminClientProvider.class);
    doReturn(topicAdminClient).when(provider).getTopicAdminClient();
    connection.setTopicAdminClientProvider(provider);
    connection.setProjectName(PROJECT);
    GoogleCloudPubSubProducer producer = new GoogleCloudPubSubProducer();
    producer.setTopic(TOPIC);
    producer.registerConnection(connection);
    producer.setCreateTopic(true);

    final AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage();
    producer.init();
    producer.start();
    try {
      TopicName topicName = producer.createOrGetTopicName(msg, producer.endpoint(msg));
      fail("No exception thrown");
    } catch (CoreException e) {
      assertEquals("Failed to get Topic", e.getMessage());
    }
    producer.stop();
    producer.close();
  }

  @Test
  @SuppressWarnings("all")
  public void testPublish() throws Exception {
    PublishResponse expectedResponse = PublishResponse.newBuilder().addMessageIds("1234").build();
    mockPublisher.addResponse(expectedResponse);
    mockPublisher.addResponse(expectedResponse);

    GoogleCloudPubSubConnection connection = new GoogleCloudPubSubConnection();
    TopicAdminClientProvider provider = mock(TopicAdminClientProvider.class);
    doReturn(topicAdminClient).when(provider).getTopicAdminClient();
    connection.setTopicAdminClientProvider(provider);
    connection.setProjectName(PROJECT);
    connection.setCredentialsProvider(new MockCredentialsProvider(credentialsProvider));
    connection.setChannelProvider(new MockChannelProvider(channelProvider));

    GoogleCloudPubSubProducer producer = new GoogleCloudPubSubProducer();
    producer.setTopic(TOPIC);
    producer.registerConnection(connection);
    producer.setCreateTopic(false);

    connection.initConnection();

    producer.init();
    producer.start();
    final AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage("Hello World");
    msg.addMetadata("foo", "bar");
    producer.produce(msg);
    producer.produce(msg);
    assertEquals(1, producer.getPublisherCache().size());
    producer.stop();
    producer.close();

    List<AbstractMessage> actualRequests = mockPublisher.getRequests();
    assertEquals(2, actualRequests.size());

    PublishRequest actualRequest = (PublishRequest) actualRequests.get(0);
    assertEquals(String.format("projects/%s/topics/%s", PROJECT, TOPIC), actualRequest.getTopic());
    assertEquals(1, actualRequest.getMessagesCount());
    assertEquals("Hello World", new String(actualRequest.getMessages(0).getData().toByteArray()));
    assertEquals(1, actualRequest.getMessages(0).getAttributesMap().size());
    assertTrue(actualRequest.getMessages(0).getAttributesMap().containsKey("foo"));
    assertEquals("bar", actualRequest.getMessages(0).getAttributesMap().get("foo"));
  }

  @Test
  @SuppressWarnings("all")
  public void testPublish_Exception() throws Exception {
    StatusRuntimeException exception = new StatusRuntimeException(Status.UNAUTHENTICATED);
    mockPublisher.addException(exception);

    GoogleCloudPubSubConnection connection = new GoogleCloudPubSubConnection();
    TopicAdminClientProvider provider = mock(TopicAdminClientProvider.class);
    doReturn(topicAdminClient).when(provider).getTopicAdminClient();
    connection.setTopicAdminClientProvider(provider);
    connection.setProjectName(PROJECT);
    connection.setCredentialsProvider(new MockCredentialsProvider(credentialsProvider));
    connection.setChannelProvider(new MockChannelProvider(channelProvider));

    GoogleCloudPubSubProducer producer = new GoogleCloudPubSubProducer();
    producer.setTopic(TOPIC);
    producer.registerConnection(connection);
    producer.setCreateTopic(false);

    connection.initConnection();

    producer.init();
    producer.start();
    final AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage("Hello World");
    msg.addMetadata("foo", "bar");
    try {
      producer.produce(msg);
      fail();
    } catch (ProduceException expected) {
    }
    producer.stop();
    producer.close();
  }

}
