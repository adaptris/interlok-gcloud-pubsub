package com.adaptris.google.cloud.pubsub;

import com.adaptris.core.AdaptrisMessageConsumerImp;
import com.adaptris.core.CoreException;
import com.adaptris.util.TimeInterval;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.util.concurrent.TimeUnit;

import static java.lang.Math.toIntExact;

public abstract class ConsumeConfig extends AdaptrisMessageConsumerImp {

  @NotNull
  @Valid
  private String subscriptionName;

  @NotNull
  @Valid
  private TimeInterval ackDeadline;

  @NotNull
  @Valid
  private Boolean createSubscription = true;

  @NotNull
  @Valid
  private Boolean autoAcknowledge = true;

  public ConsumeConfig(){
    setAckDeadline(new TimeInterval(10L, TimeUnit.SECONDS));
  }

  @Override
  public void prepare() throws CoreException {
    if(getSubscriptionName() == null){
      throw new CoreException("Subscription Name is invalid");
    }
    if(getDestination() == null){
      throw new CoreException("Destination is invalid");
    }
    if(getAckDeadline() == null){
      throw new CoreException("Ack Deadline is invalid");
    }
    if(getCreateSubscription() == null){
      throw new CoreException("Create Subscription is invalid");
    }
    if(getAutoAcknowledge() == null){
      throw new CoreException("Auto Acknowledge is invalid");
    }
  }

  public String getSubscriptionName() {
    return subscriptionName;
  }

  public void setSubscriptionName(String subscriptionName) {
    this.subscriptionName = subscriptionName;
  }

  public TimeInterval getAckDeadline() {
    return ackDeadline;
  }

  public void setAckDeadline(TimeInterval ackDeadline) {
    this.ackDeadline = ackDeadline;
  }

  public Boolean getCreateSubscription() {
    return createSubscription;
  }

  public void setCreateSubscription(Boolean createSubscription) {
    this.createSubscription = createSubscription;
  }

  public Boolean getAutoAcknowledge() {
    return autoAcknowledge;
  }

  public void setAutoAcknowledge(Boolean autoAcknowledge) {
    this.autoAcknowledge = autoAcknowledge;
  }

  public String getTopicName() {
    return getDestination().getDestination();
  }

  public int getAckDeadlineSeconds() {
    return toIntExact(TimeUnit.MILLISECONDS.toSeconds(ackDeadline.toMilliseconds()));
  }
}
