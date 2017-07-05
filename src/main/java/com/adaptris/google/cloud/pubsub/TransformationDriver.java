package com.adaptris.google.cloud.pubsub;

import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.ServiceException;
import com.adaptris.core.metadata.MetadataFilter;

/**
 * Transformation driver.
 */
public interface TransformationDriver {
  /**
   * Perform transformation.
   *
   * @param input
   *          The data to transform.
   * @param direction
   *          The direction of the transformation.
   * @param metadataFilter
   *          The filter to apply when transforming.
   * @throws ServiceException
   *           Thrown if there is a problem with the transformation.
   */
  public void transform(AdaptrisMessage input, TransformationDirection direction, MetadataFilter metadataFilter) throws ServiceException;
}
