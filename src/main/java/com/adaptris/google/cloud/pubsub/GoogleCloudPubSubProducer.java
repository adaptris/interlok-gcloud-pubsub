package com.adaptris.google.cloud.pubsub;

import com.adaptris.annotation.AdvancedConfig;
import com.adaptris.annotation.InputFieldDefault;
import com.adaptris.core.*;
import com.adaptris.core.metadata.MetadataFilter;
import com.adaptris.core.metadata.NoOpMetadataFilter;
import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.grpc.ApiException;
import com.google.api.gax.grpc.ChannelProvider;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.Topic;
import com.google.pubsub.v1.TopicName;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import io.grpc.Status;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.io.IOException;

@XStreamAlias("google-cloud-pubsub-producer")
public class GoogleCloudPubSubProducer extends ProduceOnlyProducerImp {

  @Valid
  @NotNull
  @InputFieldDefault(value = "true")
  private Boolean createTopic;

  @Valid
  @NotNull
  @AdvancedConfig
  private MetadataFilter metadataFilter = new NoOpMetadataFilter();

  private transient CredentialsProvider credentialsProvider;
  private transient ChannelProvider channelProvider;
  private transient String projectName;
  private transient TopicAdminClient topicAdminClient;

  public GoogleCloudPubSubProducer() {
    setMetadataFilter(new NoOpMetadataFilter());
    setCreateTopic(true);
  }

  @Override
  public void produce(AdaptrisMessage adaptrisMessage, ProduceDestination produceDestination) throws ProduceException {
    Publisher publisher = null;
    try {
      Topic topic = createOrGetTopic(adaptrisMessage);
      publisher = Publisher.defaultBuilder(topic.getNameAsTopicName())
          .setChannelProvider(channelProvider)
          .setCredentialsProvider(credentialsProvider)
          .build();
      publisher.publish(createPubsubMessage(adaptrisMessage));
    } catch (IOException | CoreException e) {
      throw new ProduceException(e);
    } finally {
      if (publisher != null){
        try {
          publisher.shutdown();
        } catch (Exception e) {
          log.debug("Publisher failed to shutdown", e);
        }
      }
    }
  }

  private Topic createOrGetTopic(AdaptrisMessage adaptrisMessage) throws CoreException {
    TopicName topicName = TopicName.create(projectName, getDestination().getDestination(adaptrisMessage));
    try {
      return topicAdminClient.getTopic(topicName);
    } catch (ApiException e) {
      if (Status.Code.NOT_FOUND != e.getStatusCode()) {
        throw e;
      } else {
        if (getCreateTopic()) {
          return topicAdminClient.createTopic(topicName);
        } else {
          throw e;
        }
      }
    }
  }

  private PubsubMessage createPubsubMessage(AdaptrisMessage adaptrisMessage){
    ByteString byteString = ByteString.copyFrom(adaptrisMessage.getPayload());
    PubsubMessage.Builder psmBuilder = PubsubMessage.newBuilder().setData(byteString);
    MetadataCollection filtered = getMetadataFilter().filter(adaptrisMessage);
    for (MetadataElement e : filtered){
      psmBuilder.putAttributes(e.getKey(), e.getValue());
    }
    return psmBuilder.build();
  }

  @Override
  public void prepare() throws CoreException {
    if (getCreateTopic() == null){
      throw new CoreException("create-topic is invalid");
    }
    if(getMetadataFilter() == null){
      throw new CoreException("metadata-filter is invalid");
    }
  }

  @Override
  public void init() throws CoreException {
    GoogleCloudPubSubConnection connection = retrieveConnection(GoogleCloudPubSubConnection.class);
    channelProvider = connection.getGoogleChannelProvider();
    credentialsProvider = connection.getGoogleCredentialsProvider();
    projectName = connection.getProjectName();
    topicAdminClient = connection.getTopicAdminClient();
  }

  @Override
  public void start() throws CoreException {

  }

  @Override
  public void stop() {

  }

  @Override
  public void close() {

  }

  public void setCreateTopic(Boolean createTopic) {
    this.createTopic = createTopic;
  }

  public Boolean getCreateTopic() {
    return createTopic;
  }

  public void setMetadataFilter(MetadataFilter metadataFilter) {
    this.metadataFilter = metadataFilter;
  }

  public MetadataFilter getMetadataFilter() {
    return metadataFilter;
  }
}
