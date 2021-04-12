package com.adaptris.google.cloud.pubsub.flowcontrol;

import com.adaptris.core.CoreException;
import com.google.api.gax.batching.FlowControlSettings;
import com.google.api.gax.batching.FlowController;
import com.google.cloud.pubsub.v1.Subscriber;
import com.thoughtworks.xstream.annotations.XStreamAlias;

/**
 * @author mwarman
 * @config custom-flow-control-provider
 */
@XStreamAlias("custom-flow-control-provider")
public class CustomFlowControlProvider extends DefaultFlowControlProvider {

  private FlowController.LimitExceededBehavior limitExceededBehavior;
  private Long maxOutstandingElementCount;
  private Long maxOutstandingRequestBytes;

  private transient FlowControlSettings flowControlSettings;

  @Override
  public void init() throws CoreException {
    FlowControlSettings.Builder flowControlSettingsBuilder = FlowControlSettings.newBuilder();
    if (getLimitExceededBehavior() != null) {
      flowControlSettingsBuilder.setLimitExceededBehavior(getLimitExceededBehavior());
    }
    if (getMaxOutstandingElementCount() != null){
      flowControlSettingsBuilder.setMaxOutstandingElementCount(getMaxOutstandingElementCount());
    }
    if (getMaxOutstandingRequestBytes() != null) {
      flowControlSettingsBuilder.setMaxOutstandingRequestBytes(getMaxOutstandingRequestBytes());
    }
    flowControlSettings = flowControlSettingsBuilder.build();
  }

  @Override
  public void apply(Subscriber.Builder subscriberBuilder){
    subscriberBuilder.setFlowControlSettings(getFlowControlSettings());
  }

  public FlowController.LimitExceededBehavior getLimitExceededBehavior() {
    return limitExceededBehavior;
  }

  public void setLimitExceededBehavior(FlowController.LimitExceededBehavior limitExceededBehavior) {
    this.limitExceededBehavior = limitExceededBehavior;
  }

  public Long getMaxOutstandingElementCount() {
    return maxOutstandingElementCount;
  }

  public void setMaxOutstandingElementCount(Long maxOutstandingElementCount) {
    this.maxOutstandingElementCount = maxOutstandingElementCount;
  }

  public Long getMaxOutstandingRequestBytes() {
    return maxOutstandingRequestBytes;
  }

  public void setMaxOutstandingRequestBytes(Long maxOutstandingRequestBytes) {
    this.maxOutstandingRequestBytes = maxOutstandingRequestBytes;
  }

  FlowControlSettings getFlowControlSettings() {
    return flowControlSettings;
  }
}
