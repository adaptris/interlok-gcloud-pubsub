package com.adaptris.google.cloud.pubsub;

import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.ServiceException;
import com.adaptris.core.metadata.MetadataFilter;
import com.thoughtworks.xstream.annotations.XStreamAlias;

/**
 * @config default-google-cloud-pubsub-tranform-driver
 */
@XStreamAlias("default-google-cloud-pubsub-tranform-driver")
public class DefaultGoogleCloudPubSubTransformationDriver implements TransformationDriver {

  @Override
  public void transform(AdaptrisMessage input, TransformationDirection direction, MetadataFilter metadataFilter) throws ServiceException {
    direction.transform(input, metadataFilter);
  }
}
