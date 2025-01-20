package com.adaptris.google.cloud.pubsub;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import com.adaptris.annotation.ComponentProfile;
import com.adaptris.core.AdaptrisMessage;
import com.thoughtworks.xstream.annotations.XStreamAlias;

/**
 * @config gcloud-metadata-reply-provider
 */
@XStreamAlias("gcloud-metadata-reply-provider")
@ComponentProfile(summary = "Derive a ack/nack based on metadata for GoogleCloudPubSubResponseProducer")
public class MetadataReplyProvider implements ReplyProvider {

  @NotBlank
  private String metadataKey;

  @NotNull
  private AckReply defaultReply;

  public MetadataReplyProvider() {
    this(null);
  }

  public MetadataReplyProvider(String metadataKey) {
    this(metadataKey, AckReply.NACK);
  }

  public MetadataReplyProvider(String metadataKey, AckReply defaultReply) {
    setMetadataKey(metadataKey);
    setDefaultReply(defaultReply);
  }

  @Override
  public AckReply getAckReply(AdaptrisMessage message) {
    return AckReply.valueOf(message.getMetadataValue(getMetadataKey()), getDefaultReply());
  }

  public void setMetadataKey(String metadataKey) {
    this.metadataKey = metadataKey;
  }

  public String getMetadataKey() {
    return metadataKey;
  }

  public void setDefaultReply(AckReply defaultReply) {
    this.defaultReply = defaultReply;
  }

  public AckReply getDefaultReply() {
    return defaultReply;
  }

}
