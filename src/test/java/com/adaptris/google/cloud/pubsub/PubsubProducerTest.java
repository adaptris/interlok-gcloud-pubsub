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

import com.adaptris.core.*;
import com.adaptris.google.cloud.pubsub.adminclient.TopicAdminClientProvider;
import com.adaptris.google.cloud.pubsub.channel.MockChannelProvider;
import com.adaptris.google.cloud.pubsub.credentials.MockCredentialsProvider;
import com.google.protobuf.Empty;
import com.google.protobuf.GeneratedMessageV3;
import com.google.pubsub.v1.*;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.List;

import static org.junit.Assert.*;

public class PubsubProducerTest extends ServiceHelperBase {


  @Test
  @SuppressWarnings("all")
  public void testCreateOrGetTopicName_GetNoCheck() throws Exception {
    Empty expectedResponse = Empty.newBuilder().build();
    mockPublisher.addResponse(expectedResponse);

    GoogleCloudPubSubConnection connection = new GoogleCloudPubSubConnection();
    TopicAdminClientProvider provider = Mockito.mock(TopicAdminClientProvider.class);
    Mockito.doReturn(topicAdminClient).when(provider).getTopicAdminClient();
    connection.setTopicAdminClientProvider(provider);
    connection.setProjectName(PROJECT);
    GoogleCloudPubSubProducer producer = new GoogleCloudPubSubProducer();
    producer.setDestination(new ConfiguredProduceDestination(TOPIC));
    producer.registerConnection(connection);
    producer.setCreateTopic(false);

    final AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage();
    producer.init();
    producer.start();
    ProjectTopicName topicName = producer.createOrGetTopicName(msg);
    producer.stop();
    producer.close();

    List<GeneratedMessageV3> actualRequests = mockPublisher.getRequests();
    assertEquals(0, actualRequests.size());

    assertEquals(TOPIC, topicName.getTopic());
    assertEquals(PROJECT, topicName.getProject());
  }

  @Test
  @SuppressWarnings("all")
  public void testCreateOrGetTopicName_GetWithCreate() throws Exception {
    Topic expectedResponse = Topic.newBuilder()
        .setName(ProjectTopicName.of(PROJECT, TOPIC).toString())
        .build();
    mockPublisher.addResponse(expectedResponse);

    GoogleCloudPubSubConnection connection = new GoogleCloudPubSubConnection();
    TopicAdminClientProvider provider = Mockito.mock(TopicAdminClientProvider.class);
    Mockito.doReturn(topicAdminClient).when(provider).getTopicAdminClient();
    connection.setTopicAdminClientProvider(provider);
    connection.setProjectName(PROJECT);
    GoogleCloudPubSubProducer producer = new GoogleCloudPubSubProducer();
    producer.setDestination(new ConfiguredProduceDestination(TOPIC));
    producer.registerConnection(connection);
    producer.setCreateTopic(true);

    final AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage();
    producer.init();
    producer.start();
    ProjectTopicName topicName = producer.createOrGetTopicName(msg);
    producer.stop();
    producer.close();

    List<GeneratedMessageV3> actualRequests = mockPublisher.getRequests();
    assertEquals(1, actualRequests.size());

    assertEquals(TOPIC, topicName.getTopic());
    assertEquals(PROJECT, topicName.getProject());
  }

  @Test
  @SuppressWarnings("all")
  public void testCreateOrGetTopicName_GetWithCreate_NotFound() throws Exception {
    StatusRuntimeException exception = new StatusRuntimeException(Status.NOT_FOUND);
    mockPublisher.addException(exception);
    Topic expectedResponse = Topic.newBuilder()
        .setName(ProjectTopicName.of(PROJECT, TOPIC).toString())
        .build();
    mockPublisher.addResponse(expectedResponse);

    GoogleCloudPubSubConnection connection = new GoogleCloudPubSubConnection();
    TopicAdminClientProvider provider = Mockito.mock(TopicAdminClientProvider.class);
    Mockito.doReturn(topicAdminClient).when(provider).getTopicAdminClient();
    connection.setTopicAdminClientProvider(provider);
    connection.setProjectName(PROJECT);
    GoogleCloudPubSubProducer producer = new GoogleCloudPubSubProducer();
    producer.setDestination(new ConfiguredProduceDestination(TOPIC));
    producer.registerConnection(connection);
    producer.setCreateTopic(true);

    final AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage();
    producer.init();
    producer.start();
    ProjectTopicName topicName = producer.createOrGetTopicName(msg);
    producer.stop();
    producer.close();

    List<GeneratedMessageV3> actualRequests = mockPublisher.getRequests();
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
    TopicAdminClientProvider provider = Mockito.mock(TopicAdminClientProvider.class);
    Mockito.doReturn(topicAdminClient).when(provider).getTopicAdminClient();
    connection.setTopicAdminClientProvider(provider);
    connection.setProjectName(PROJECT);
    GoogleCloudPubSubProducer producer = new GoogleCloudPubSubProducer();
    producer.setDestination(new ConfiguredProduceDestination(TOPIC));
    producer.registerConnection(connection);
    producer.setCreateTopic(true);

    final AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage();
    producer.init();
    producer.start();
    try {
      TopicName topicName = producer.createOrGetTopicName(msg);
      fail("No exception thrown");
    } catch (CoreException e){
      assertEquals("Failed to get Topic", e.getMessage());
    }
    producer.stop();
    producer.close();
  }


  @Test
  @SuppressWarnings("all")
  public void testPublish() throws Exception {
    PublishResponse expectedResponse = PublishResponse.newBuilder()
        .addMessageIds("1234")
        .build();
    mockPublisher.addResponse(expectedResponse);
    mockPublisher.addResponse(expectedResponse);

    GoogleCloudPubSubConnection connection = new GoogleCloudPubSubConnection();
    TopicAdminClientProvider provider = Mockito.mock(TopicAdminClientProvider.class);
    Mockito.doReturn(topicAdminClient).when(provider).getTopicAdminClient();
    connection.setTopicAdminClientProvider(provider);
    connection.setProjectName(PROJECT);
    connection.setCredentialsProvider(new MockCredentialsProvider(credentialsProvider));
    connection.setChannelProvider(new MockChannelProvider(channelProvider));

    GoogleCloudPubSubProducer producer = new GoogleCloudPubSubProducer();
    producer.setDestination(new ConfiguredProduceDestination(TOPIC));
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

    List<GeneratedMessageV3> actualRequests = mockPublisher.getRequests();
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
    TopicAdminClientProvider provider = Mockito.mock(TopicAdminClientProvider.class);
    Mockito.doReturn(topicAdminClient).when(provider).getTopicAdminClient();
    connection.setTopicAdminClientProvider(provider);
    connection.setProjectName(PROJECT);
    connection.setCredentialsProvider(new MockCredentialsProvider(credentialsProvider));
    connection.setChannelProvider(new MockChannelProvider(channelProvider));

    GoogleCloudPubSubProducer producer = new GoogleCloudPubSubProducer();
    producer.setDestination(new ConfiguredProduceDestination(TOPIC));
    producer.registerConnection(connection);
    producer.setCreateTopic(false);

    connection.initConnection();

    producer.init();
    producer.start();
    final AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage("Hello World");
    msg.addMetadata("foo", "bar");
    try {
      producer.produce(msg);
    } catch (ProduceException e){
      assertEquals("Failed to Produce Message", e.getMessage());
    }
    producer.stop();
    producer.close();
  }


}
