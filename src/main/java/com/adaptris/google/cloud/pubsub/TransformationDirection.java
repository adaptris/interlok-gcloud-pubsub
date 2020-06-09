package com.adaptris.google.cloud.pubsub;

import java.io.IOException;
import java.io.Reader;
import com.adaptris.annotation.InputFieldHint;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.MetadataCollection;
import com.adaptris.core.MetadataElement;
import com.adaptris.core.ServiceException;
import com.adaptris.core.metadata.MetadataFilter;
import com.adaptris.core.util.MetadataHelper;
import com.adaptris.util.KeyValuePairBag;
import com.adaptris.util.KeyValuePairSet;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.google.pubsub.v1.PublishRequest;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.ReceivedMessage;

/**
 * Direction enum; Google Cloud Pub/Sub JSON <-> Interlok Messsage.
 */
public enum TransformationDirection {

  /**
   * Interlok Message to Publish Request
   */
  @InputFieldHint(friendly = "Interlok Message to Publish Request")
  INTERLOK_TO_PUBLISH_REQUEST {
    @Override
    public void transform(AdaptrisMessage message, MetadataFilter metadataFilter) throws ServiceException {
      try {
        ByteString byteString = ByteString.copyFrom(message.getPayload());
        PubsubMessage.Builder psmBuilder = PubsubMessage.newBuilder().setData(byteString);
        MetadataCollection filtered = metadataFilter.filter(message);
        for (MetadataElement e : filtered){
          psmBuilder.putAttributes(e.getKey(), e.getValue());
        }
        PublishRequest.Builder publishRequest = PublishRequest.newBuilder();
        publishRequest.addMessages(psmBuilder.build());
        String json = JsonFormat.printer().print(publishRequest.build());
        message.setPayload(json.getBytes());
      } catch (InvalidProtocolBufferException e) {
        throw new ServiceException(e);
      }
    }
  },


  /**
   * Pull Response to Interlok Message
   */
  @InputFieldHint(friendly = "Pull Response to Interlok Message")
  PULL_RESPONSE_TO_INTERLOK {
    @Override
    public void transform(AdaptrisMessage message, MetadataFilter metadataFilter) throws ServiceException {
      try {
        PullResponse.Builder pullResponse = PullResponse.newBuilder();
        JsonFormat.parser().merge(new String(message.getPayload()), pullResponse);
        if (pullResponse.getReceivedMessagesCount() != 1) {
          throw new ServiceException("Pull Response can only contain one message");
        }
        ReceivedMessage receivedMessage = pullResponse.getReceivedMessages(0);
        message.addMetadata("gcloud_ackId", receivedMessage.getAckId());
        pubSubMessageParser(message, metadataFilter, receivedMessage.getMessage());
      } catch (IOException e){
        throw new ServiceException(e);
      }
    }
  },


  /**
   * Push Response to Interlok Message
   */
  @InputFieldHint(friendly = "Push Response to Interlok Message")
  PUSH_RESPONSE_TO_INTERLOK {
    @Override
    public void transform(AdaptrisMessage message, MetadataFilter metadataFilter) throws ServiceException {
      try (Reader reader = message.getReader()) {
        JsonElement jsonRoot = JsonParser.parseReader(reader);
        String messageStr = jsonRoot.getAsJsonObject().get("message").toString();
        message.addMetadata("gcloud_subscription", jsonRoot.getAsJsonObject().get("subscription").toString());
        PubsubMessage.Builder pubsubMessage = PubsubMessage.newBuilder();
        JsonFormat.parser().merge(messageStr, pubsubMessage);
        pubSubMessageParser(message, metadataFilter, pubsubMessage.build());
      } catch (IOException e){
        throw new ServiceException(e);
      }
    }
  };

  public abstract void transform(AdaptrisMessage message, MetadataFilter metadataFilter) throws ServiceException;

  private static void pubSubMessageParser(AdaptrisMessage message, MetadataFilter metadataFilter, PubsubMessage pubsubMessage) throws IOException {
    MetadataCollection filtered = metadataFilter.filter(MetadataHelper.convertFromProperties(KeyValuePairBag.asProperties(new KeyValuePairSet(pubsubMessage.getAttributesMap()))));
    for (MetadataElement e : filtered) {
      message.addMetadata(e);
    }
    message.addMetadata("gcloud_messageId", pubsubMessage.getMessageId());
    if(pubsubMessage.hasPublishTime()) {
      message.addMetadata("gcloud_publishTimeSeconds", String.valueOf(pubsubMessage.getPublishTime().getSeconds()));
    }
    message.setPayload(pubsubMessage.getData().toByteArray());
  }
}
