
package com.adaptris.google.cloud.pubsub;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;

import com.adaptris.annotation.AdvancedConfig;
import com.adaptris.annotation.AutoPopulated;
import com.adaptris.annotation.ComponentProfile;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.ServiceException;
import com.adaptris.core.ServiceImp;
import com.adaptris.core.metadata.MetadataFilter;
import com.adaptris.core.metadata.NoOpMetadataFilter;
import com.thoughtworks.xstream.annotations.XStreamAlias;

/**
 * @config google-cloud-pubsub-transform-service
 */
@XStreamAlias("google-cloud-pubsub-transform-service")
@ComponentProfile(summary = "Map from google pubsub message format to an Interlok message", tag = "service,gcloud")
public class GoogleCloudPubSubTransformService extends ServiceImp {

  @AutoPopulated
  @NotNull
  @Valid
  private TransformationDirection direction = TransformationDirection.INTERLOK_TO_PUBLISH_REQUEST;

  @AutoPopulated
  @AdvancedConfig
  @NotNull
  @Valid
  private TransformationDriver driver = new DefaultGoogleCloudPubSubTransformationDriver();

  @AdvancedConfig
  private MetadataFilter metadataFilter = new NoOpMetadataFilter();

  public GoogleCloudPubSubTransformService() {
    super();
    setMetadataFilter(new NoOpMetadataFilter());
  }

  public GoogleCloudPubSubTransformService(TransformationDirection direction) {
    this(direction, new DefaultGoogleCloudPubSubTransformationDriver());
  }

  public GoogleCloudPubSubTransformService(TransformationDirection direction, TransformationDriver driver) {
    this();
    setDriver(driver);
    setDirection(direction);
  }

  @Override
  public void doService(AdaptrisMessage msg) throws ServiceException {
    getDriver().transform(msg, getDirection(), getMetadataFilter());
  }

  @Override
  public final void prepare() {
  }

  @Override
  public final void initService() {
  }

  @Override
  public final void closeService() {
  }

  public void setDirection(TransformationDirection direction) {
    this.direction = direction;
  }

  public TransformationDirection getDirection() {
    return direction;
  }

  public void setDriver(TransformationDriver driver) {
    this.driver = driver;
  }

  public TransformationDriver getDriver() {
    return driver;
  }

  public void setMetadataFilter(MetadataFilter metadataFilter) {
    this.metadataFilter = metadataFilter;
  }

  public MetadataFilter getMetadataFilter() {
    return metadataFilter;
  }

}