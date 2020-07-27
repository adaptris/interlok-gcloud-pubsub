package com.adaptris.google.cloud.pubsub;

import static com.adaptris.core.util.DestinationHelper.logWarningIfNotNull;
import static com.adaptris.core.util.DestinationHelper.mustHaveEither;
import static com.adaptris.core.util.DestinationHelper.resolveProduceDestination;
import java.util.Map;
import javax.validation.Valid;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.ObjectUtils;
import com.adaptris.annotation.AdvancedConfig;
import com.adaptris.annotation.ComponentProfile;
import com.adaptris.annotation.DisplayOrder;
import com.adaptris.annotation.InputFieldDefault;
import com.adaptris.annotation.InputFieldHint;
import com.adaptris.annotation.Removal;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.CoreException;
import com.adaptris.core.MetadataCollection;
import com.adaptris.core.MetadataElement;
import com.adaptris.core.ProduceDestination;
import com.adaptris.core.ProduceException;
import com.adaptris.core.ProduceOnlyProducerImp;
import com.adaptris.core.metadata.MetadataFilter;
import com.adaptris.core.metadata.NoOpMetadataFilter;
import com.adaptris.core.util.ExceptionHelper;
import com.adaptris.core.util.LoggingHelper;
import com.adaptris.util.NumberUtils;
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
import com.thoughtworks.xstream.annotations.XStreamAlias;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@XStreamAlias("google-cloud-pubsub-producer")
@ComponentProfile(summary = "Publish a message to Google pubsub", tag = "producer,gcloud,messaging", recommended =
{
    GoogleCloudPubSubConnection.class
})
@DisplayOrder(order =
{
    "topic", "createTopic", "publisherCacheLimit", "metadataFilter"
})
@NoArgsConstructor
public class GoogleCloudPubSubProducer extends ProduceOnlyProducerImp {

  @InputFieldDefault(value = "false")
  @Getter
  @Setter
  private Boolean createTopic;

  @Getter
  @Setter
  @InputFieldDefault(value = "PublisherMap#DEFAULT_MAX_ENTRIES (10)")
  private Integer publisherCacheLimit;

  @Valid
  @AdvancedConfig
  @Getter
  @Setter
  private MetadataFilter metadataFilter;

  /**
   * The destination is the topic.
   *
   */
  @Getter
  @Setter
  @Deprecated
  @Valid
  @Removal(version = "4.0.0", message = "Use 'topic' instead")
  private ProduceDestination destination;

  /**
   * The pubsub topic
   *
   */
  @InputFieldHint(expression = true)
  @Getter
  @Setter
  // Needs to be @NotBlank when destination is removed.
  private String topic;

  private transient boolean destWarning;

  private transient CredentialsProvider credentialsProvider;
  private transient TransportChannelProvider channelProvider;
  private transient String projectName;
  private transient TopicAdminClient topicAdminClient;
  @Getter(AccessLevel.PACKAGE)
  @Setter(AccessLevel.PACKAGE)
  private transient Map<String, Publisher> publisherCache;

  @Override
  protected void doProduce(AdaptrisMessage adaptrisMessage, String endpoint)
      throws ProduceException {
    try {
      Publisher publisher;
      if (publisherCache.containsKey(endpoint)) {
        log.trace("Found publisher for key [{}]", endpoint);
        publisher = publisherCache.get(endpoint);
      } else {
        log.trace("No publisher found for key [{}]", endpoint);
        publisher = Publisher.newBuilder(createOrGetTopicName(adaptrisMessage, endpoint))
            .setChannelProvider(channelProvider)
            .setCredentialsProvider(credentialsProvider)
            .build();
        publisherCache.put(endpoint, publisher);
      }
      ApiFuture<String> messageId = publisher.publish(createPubsubMessage(adaptrisMessage));
      log.debug("Published with message ID: {}", messageId.get());
    } catch (Exception e) {
      throw ExceptionHelper.wrapProduceException(e);
    }
  }

  @SuppressWarnings("deprecation")
  ProjectTopicName createOrGetTopicName(AdaptrisMessage adaptrisMessage, String endpoint)
      throws CoreException {
    ProjectTopicName topicName = ProjectTopicName.of(projectName, endpoint);
    if (!createTopic()) {
      return topicName;
    }
    // could cast to TopicName since ProjectTopicName extends TopicName to avoid the
    // deprecation warning
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
    MetadataCollection filtered = metadataFilter().filter(adaptrisMessage);
    for (MetadataElement e : filtered){
      psmBuilder.putAttributes(e.getKey(), e.getValue());
    }
    return psmBuilder.build();
  }

  @Override
  public void prepare() throws CoreException {
    logWarningIfNotNull(destWarning, () -> destWarning = true, getDestination(),
        "{} uses destination, use 'topic' instead", LoggingHelper.friendlyName(this));
    mustHaveEither(getTopic(), getDestination());
  }

  @Override
  public void init() throws CoreException {
    GoogleCloudPubSubConnection connection = retrieveConnection(GoogleCloudPubSubConnection.class);
    channelProvider = connection.getGoogleChannelProvider();
    credentialsProvider = connection.getGoogleCredentialsProvider();
    projectName = connection.getProjectName();
    topicAdminClient = connection.getTopicAdminClient();
    publisherCache = new PublisherMap(publisherCacheLimit());
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

  @SuppressWarnings("unchecked")
  public <T extends GoogleCloudPubSubProducer> T withTopic(String s) {
    setTopic(s);
    return (T) this;
  }

  private boolean createTopic() {
    return BooleanUtils.toBooleanDefaultIfNull(getCreateTopic(), false);
  }

  private MetadataFilter metadataFilter() {
    return ObjectUtils.defaultIfNull(getMetadataFilter(), new NoOpMetadataFilter());
  }

  private int publisherCacheLimit() {
    return NumberUtils.toIntDefaultIfNull(getPublisherCacheLimit(),
        PublisherMap.DEFAULT_MAX_ENTRIES);
  }

  @Override
  public String endpoint(AdaptrisMessage msg) throws ProduceException {
    return resolveProduceDestination(getTopic(), getDestination(), msg);
  }

}
