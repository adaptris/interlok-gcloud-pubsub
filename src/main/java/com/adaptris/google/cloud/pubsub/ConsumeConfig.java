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
import com.adaptris.core.util.DestinationHelper;
import com.adaptris.util.TimeInterval;

import lombok.Getter;
import lombok.Setter;

public abstract class ConsumeConfig extends AdaptrisMessageConsumerImp {

  @Getter
  @Setter
  @NotNull
  @Valid
  private String subscriptionName;

  @Getter
  @Setter
  @NotNull
  @Valid
  @AutoPopulated
  @InputFieldDefault(value = "10 seconds")
  private TimeInterval ackDeadline;

  @Getter
  @Setter
  @NotNull
  @Valid
  @AutoPopulated
  @InputFieldDefault(value = "true")
  private Boolean createSubscription = true;

  @Getter
  @Setter
  @NotNull
  @Valid
  @AutoPopulated
  @InputFieldDefault(value = "true")
  private Boolean autoAcknowledge = true;

  /**
   * The pubsub topic name.
   *
   */
  @Getter
  @Setter
  private String topic;
  
  public ConsumeConfig(){
    setAckDeadline(new TimeInterval(10L, TimeUnit.SECONDS));
  }

  @Override
  public void prepare() throws CoreException {
    Args.notNull(getSubscriptionName(), "subscriptionName");
    Args.notNull(getAckDeadline(), "ackDeadline");
    Args.notNull(getCreateSubscription(), "createSubscription");
    Args.notNull(getAutoAcknowledge(), "autoAcknowledge");
  }

  public String getTopicName() {
    return getTopic();
  }

  public int getAckDeadlineSeconds() {
    return toIntExact(TimeUnit.MILLISECONDS.toSeconds(ackDeadline.toMilliseconds()));
  }


  @Override
  protected String newThreadName() {
    return DestinationHelper.threadName(retrieveAdaptrisMessageListener());
  }
}
