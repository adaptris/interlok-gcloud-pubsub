package com.adaptris.google.cloud.pubsub;

import com.adaptris.core.AdaptrisMessage;
import com.adaptris.google.cloud.pubsub.ReplyProvider;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import org.hibernate.validator.constraints.NotBlank;

import javax.validation.constraints.NotNull;

@XStreamAlias("metadata-reply-provider")
public class MetadataReplyProvider implements ReplyProvider {

  @NotBlank
  private String metadataKey;

  @NotNull
  private AckReply defaultReply;

  public MetadataReplyProvider(){
    this(null);
  }

  public MetadataReplyProvider(String metadataKey){
    this(metadataKey, AckReply.NACK);
  }

  public MetadataReplyProvider(String metadataKey, AckReply defaultReply){
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
