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

import java.io.IOException;
import java.util.Arrays;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;

import com.adaptris.google.cloud.pubsub.mocks.MockPublisher;
import com.adaptris.google.cloud.pubsub.mocks.MockSubscriber;
import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.testing.MockGrpcService;
import com.google.api.gax.grpc.testing.MockServiceHelper;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminSettings;

public class ServiceHelperBase {

  static final String PROJECT = "project-name";
  static final String TOPIC = "topic-name";
  static final String SUBSCRIPTION = "subscription-name";

  static MockPublisher mockPublisher;
  static MockSubscriber mockSubscriber;
  // private static MockIAMPolicy mockIAMPolicy;
  static MockServiceHelper serviceHelper;

  SubscriptionAdminClient subscriptionAdminClient;
  TopicAdminClient topicAdminClient;
  TransportChannelProvider channelProvider;
  CredentialsProvider credentialsProvider;

  @BeforeAll
  public static void startStaticServer() {
    mockSubscriber = new MockSubscriber();
    mockPublisher = new MockPublisher();
    serviceHelper = new MockServiceHelper("in-process-1", Arrays.<MockGrpcService> asList(mockPublisher, mockSubscriber));
    serviceHelper.start();
  }

  @AfterAll
  public static void stopServer() {
    serviceHelper.stop();
  }

  @BeforeEach
  public void setUp() throws IOException {
    serviceHelper.reset();
    channelProvider = serviceHelper.createChannelProvider();
    credentialsProvider = new NoCredentialsProvider();
    SubscriptionAdminSettings settings = SubscriptionAdminSettings.newBuilder().setTransportChannelProvider(channelProvider)
        .setCredentialsProvider(credentialsProvider).build();
    subscriptionAdminClient = SubscriptionAdminClient.create(settings);

    TopicAdminSettings topicAdminSettings = TopicAdminSettings.newBuilder()
        .setTransportChannelProvider(serviceHelper.createChannelProvider()).setCredentialsProvider(new NoCredentialsProvider()).build();
    topicAdminClient = TopicAdminClient.create(topicAdminSettings);
  }

  @AfterEach
  public void tearDown() throws Exception {
    subscriptionAdminClient.close();
  }
}
