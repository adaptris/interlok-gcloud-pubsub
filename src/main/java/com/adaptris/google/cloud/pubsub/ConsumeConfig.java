package com.adaptris.google.cloud.pubsub;

import static java.lang.Math.toIntExact;

import java.util.concurrent.TimeUnit;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

import com.adaptris.annotation.AutoPopulated;
import com.adaptris.annotation.InputFieldDefault;
import com.adaptris.core.AdaptrisMessageConsumerImp;
import com.adaptris.core.CoreException;
import com.adaptris.core.util.Args;
import com.adaptris.core.util.ExceptionHelper;
import com.adaptris.util.TimeInterval;

public abstract class ConsumeConfig extends AdaptrisMessageConsumerImp {

  @NotNull
  @Valid
  private String subscriptionName;

  @NotNull
  @Valid
  @AutoPopulated
  @InputFieldDefault(value = "10 seconds")
  private TimeInterval ackDeadline;

  @NotNull
  @Valid
  @AutoPopulated
  @InputFieldDefault(value = "true")
  private Boolean createSubscription = true;

  @NotNull
  @Valid
  @AutoPopulated
  @InputFieldDefault(value = "true")
  private Boolean autoAcknowledge = true;

  public ConsumeConfig(){
    setAckDeadline(new TimeInterval(10L, TimeUnit.SECONDS));
  }

  @Override
  public void prepare() throws CoreException {
    try {
      Args.notNull(getSubscriptionName(), "subscriptionName");
      Args.notNull(getDestination(), "destination");
      Args.notNull(getAckDeadline(), "ackDeadline");
      Args.notNull(getCreateSubscription(), "createSubscription");
      Args.notNull(getAutoAcknowledge(), "autoAcknowledge");
    }
    catch (IllegalArgumentException e) {
      throw ExceptionHelper.wrapCoreException(e);
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
