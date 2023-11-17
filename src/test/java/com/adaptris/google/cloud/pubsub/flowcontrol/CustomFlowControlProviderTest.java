package com.adaptris.google.cloud.pubsub.flowcontrol;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

import com.google.api.gax.batching.FlowControlSettings;
import com.google.api.gax.batching.FlowController;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.pubsub.v1.ProjectSubscriptionName;

/**
 * @author mwarman
 */
public class CustomFlowControlProviderTest {

  @Test
  public void construct() throws Exception {
    CustomFlowControlProvider flowControlProvider = new CustomFlowControlProvider();
    assertNull(flowControlProvider.getMaxOutstandingRequestBytes());
    assertNull(flowControlProvider.getMaxOutstandingElementCount());
    assertNull(flowControlProvider.getLimitExceededBehavior());
  }

  @Test
  public void getMaxOutstandingRequestBytes() throws Exception {
    CustomFlowControlProvider flowControlProvider = new CustomFlowControlProvider();
    assertNull(flowControlProvider.getMaxOutstandingRequestBytes());
    flowControlProvider.setMaxOutstandingRequestBytes(1024L);
    assertEquals(1024L, flowControlProvider.getMaxOutstandingRequestBytes().longValue());
    flowControlProvider.init();
    FlowControlSettings flowControlSettings = flowControlProvider.getFlowControlSettings();
    assertEquals(1024L, flowControlSettings.getMaxOutstandingRequestBytes().longValue());
    assertEquals(FlowController.LimitExceededBehavior.Block, flowControlSettings.getLimitExceededBehavior());
    assertNull(flowControlSettings.getMaxOutstandingElementCount());
  }

  @Test
  public void getMaxOutstandingElementCount() throws Exception {
    CustomFlowControlProvider flowControlProvider = new CustomFlowControlProvider();
    assertNull(flowControlProvider.getMaxOutstandingElementCount());
    flowControlProvider.setMaxOutstandingElementCount(10L);
    assertEquals(10L, flowControlProvider.getMaxOutstandingElementCount().longValue());
    flowControlProvider.init();
    FlowControlSettings flowControlSettings = flowControlProvider.getFlowControlSettings();
    assertEquals(10L, flowControlSettings.getMaxOutstandingElementCount().longValue());
    assertEquals(FlowController.LimitExceededBehavior.Block, flowControlSettings.getLimitExceededBehavior());
    assertNull(flowControlSettings.getMaxOutstandingRequestBytes());
  }

  @Test
  public void getLimitExceededBehavior() throws Exception {
    CustomFlowControlProvider flowControlProvider = new CustomFlowControlProvider();
    assertNull(flowControlProvider.getLimitExceededBehavior());
    flowControlProvider.setLimitExceededBehavior(FlowController.LimitExceededBehavior.Ignore);
    assertEquals(FlowController.LimitExceededBehavior.Ignore, flowControlProvider.getLimitExceededBehavior());
    flowControlProvider.init();
    FlowControlSettings flowControlSettings = flowControlProvider.getFlowControlSettings();
    assertEquals(FlowController.LimitExceededBehavior.Ignore, flowControlSettings.getLimitExceededBehavior());
    assertNull(flowControlSettings.getMaxOutstandingRequestBytes());
    assertNull(flowControlSettings.getMaxOutstandingElementCount());
  }

  @Test
  public void apply() throws Exception {
    CustomFlowControlProvider flowControlProvider = new CustomFlowControlProvider();
    flowControlProvider.setMaxOutstandingElementCount(10L);
    flowControlProvider.setMaxOutstandingRequestBytes(1024L);
    flowControlProvider.setLimitExceededBehavior(FlowController.LimitExceededBehavior.Block);
    flowControlProvider.init();
    Subscriber.Builder subscriberBuilder = Subscriber.newBuilder(ProjectSubscriptionName.of("project", "subscription"),
        (MessageReceiver) (message, consumer) -> {

        });
    flowControlProvider.apply(subscriberBuilder);
    Subscriber subscriber = subscriberBuilder.build();
    assertEquals(FlowController.LimitExceededBehavior.Block, subscriber.getFlowControlSettings().getLimitExceededBehavior());
    assertEquals(10L, subscriber.getFlowControlSettings().getMaxOutstandingElementCount().longValue());
    assertEquals(1024L, subscriber.getFlowControlSettings().getMaxOutstandingRequestBytes().longValue());
  }

}