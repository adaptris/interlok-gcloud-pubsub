package com.adaptris.google.cloud.pubsub;

import com.adaptris.annotation.AdvancedConfig;
import com.adaptris.annotation.InputFieldDefault;
import com.adaptris.core.*;
import com.adaptris.core.metadata.MetadataFilter;
import com.adaptris.core.metadata.NoOpMetadataFilter;
import com.google.api.core.ApiFuture;
import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.StatusCode;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.Topic;
import com.google.pubsub.v1.TopicName;
import com.thoughtworks.xstream.annotations.XStreamAlias;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutionException;

@XStreamAlias("google-cloud-pubsub-producer")
public class GoogleCloudPubSubProducer extends ProduceOnlyProducerImp {

  @Valid
  @NotNull
  @InputFieldDefault(value = "false")
  private Boolean createTopic;

  @Valid
  @NotNull
  private Integer publisherCacheLimit;

  @Valid
  @NotNull
  @AdvancedConfig
  private MetadataFilter metadataFilter = new NoOpMetadataFilter();

  private transient CredentialsProvider credentialsProvider;
  private transient TransportChannelProvider channelProvider;
  private transient String projectName;
  private transient TopicAdminClient topicAdminClient;
  private transient Map<String, Publisher> publisherCache;

  public GoogleCloudPubSubProducer() {
    setMetadataFilter(new NoOpMetadataFilter());
    setCreateTopic(false);
    setPublisherCacheLimit(PublisherMap.DEFAULT_MAX_ENTRIES);
  }

  @Override
  public void produce(AdaptrisMessage adaptrisMessage, ProduceDestination produceDestination) throws ProduceException {
    try {
      String key = getDestination().getDestination(adaptrisMessage);
      Publisher publisher;
      if (publisherCache.containsKey(key)){
        log.trace(String.format("Found publisher for key [%s]", key));
        publisher = publisherCache.get(key);
      } else {
        log.trace(String.format("No publisher found for key [%s]", key));
        publisher = Publisher.newBuilder(createOrGetTopicName(adaptrisMessage))
            .setChannelProvider(channelProvider)
            .setCredentialsProvider(credentialsProvider)
            .build();
        publisherCache.put(key, publisher);
      }
      ApiFuture<String> messageId = publisher.publish(createPubsubMessage(adaptrisMessage));
      log.debug(String.format("Published with message ID: %s", messageId.get()));
    } catch (IOException | CoreException | InterruptedException | ExecutionException e) {
      throw new ProduceException("Failed to Produce Message", e);
    }
  }

  ProjectTopicName createOrGetTopicName(AdaptrisMessage adaptrisMessage) throws CoreException {
    ProjectTopicName topicName = ProjectTopicName.of(projectName, getDestination().getDestination(adaptrisMessage));
    if(!getCreateTopic()){
      return topicName;
    }
    try {
      Topic topic = topicAdminClient.getTopic(topicName);
      return ProjectTopicName.parse(topic.getName());
    } catch (ApiException e) {
      if (StatusCode.Code.NOT_FOUND != e.getStatusCode().getCode()) {
        throw new CoreException("Failed to get Topic", e);
      } else {
        Topic topic = topicAdminClient.createTopic(topicName);
        return ProjectTopicName.parse(topic.getName());
      }
    }
  }

  PubsubMessage createPubsubMessage(AdaptrisMessage adaptrisMessage){
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
    if(getPublisherCacheLimit() == null){
      throw new CoreException("publisher-cache-limit is invalid");
    }
  }

  @Override
  public void init() throws CoreException {
    GoogleCloudPubSubConnection connection = retrieveConnection(GoogleCloudPubSubConnection.class);
    channelProvider = connection.getGoogleChannelProvider();
    credentialsProvider = connection.getGoogleCredentialsProvider();
    projectName = connection.getProjectName();
    topicAdminClient = connection.getTopicAdminClient();
    publisherCache = new PublisherMap(getPublisherCacheLimit());
  }

  @Override
  public void start() throws CoreException {

  }

  @Override
  public void stop() {

  }

  @Override
  public void close() {
    for (Map.Entry<String, Publisher> publisherEntry : publisherCache.entrySet()) {
      if (publisherEntry.getValue() != null) {
        try {
          publisherEntry.getValue().shutdown();
        } catch (Exception e) {
          log.debug("Publisher failed to shutdown", e);
        }
      }
    }
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

  public void setPublisherCacheLimit(Integer publisherCacheLimit) {
    this.publisherCacheLimit = publisherCacheLimit;
  }

  public Integer getPublisherCacheLimit() {
    return publisherCacheLimit;
  }

  Map<String, Publisher> getPublisherCache() {
    return publisherCache;
  }

  void setPublisherCache(Map<String, Publisher> publisherCache) {
    this.publisherCache = publisherCache;
  }
}
