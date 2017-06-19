package com.adaptris.google.cloud.pubsub.transform;

import com.adaptris.annotation.InputFieldHint;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.MetadataCollection;
import com.adaptris.core.MetadataElement;
import com.adaptris.core.ServiceException;
import com.adaptris.core.metadata.MetadataFilter;
import com.adaptris.core.util.MetadataHelper;
import com.adaptris.util.KeyValuePairBag;
import com.adaptris.util.KeyValuePairSet;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.google.pubsub.v1.PubsubMessage;

import java.io.IOException;

/**
 * Direction enum; Google Cloud Pub/Sub JSON <-> Interlok Messsage.
 */
public enum TransformationDirection {

  /**
   * Google Cloud Pub/Sub JSON to Interlok Message.
   */
  @InputFieldHint(friendly = "Google Cloud Pub/Sub JSON to Interlok Message")
  PUBSUB_TO_INTERLOK {
    @Override
    public void transform(AdaptrisMessage message, MetadataFilter metadataFilter) throws ServiceException {
      try {
        PubsubMessage.Builder pubsubMessage = PubsubMessage.newBuilder();
        JsonFormat.parser().merge(new String(message.getPayload()), pubsubMessage);
        MetadataCollection filtered = metadataFilter.filter(MetadataHelper.convertFromProperties(KeyValuePairBag.asProperties(new KeyValuePairSet(pubsubMessage.getAttributesMap()))));
        for (MetadataElement e : filtered) {
          message.addMetadata(e);
        }
        message.setPayload(pubsubMessage.getData().toByteArray());
      } catch (IOException e){
        throw new ServiceException(e);
      }
    }
  },

  /**
   * Interlok Message to Google Cloud Pub/Sub JSON.
   */
  @InputFieldHint(friendly = "Interlok Message to Google Cloud Pub/Sub JSON")
  INTERLOK_TO_PUBSUB {
    @Override
    public void transform(AdaptrisMessage message, MetadataFilter metadataFilter) throws ServiceException {
      try {
        ByteString byteString = ByteString.copyFrom(message.getPayload());
        PubsubMessage.Builder psmBuilder = PubsubMessage.newBuilder().setData(byteString);
        MetadataCollection filtered = metadataFilter.filter(message);
        for (MetadataElement e : filtered){
          psmBuilder.putAttributes(e.getKey(), e.getValue());
        }
        String json = JsonFormat.printer().print(psmBuilder.build());
        message.setPayload(json.getBytes());
      } catch (InvalidProtocolBufferException e) {
        throw new ServiceException(e);
      }
    }
  };

  public abstract void transform(AdaptrisMessage message, MetadataFilter metadataFilter) throws ServiceException;
}
