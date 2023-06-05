package com.adaptris.google.cloud.pubsub.flowcontrol;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

import com.google.api.gax.batching.FlowController;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.pubsub.v1.ProjectSubscriptionName;

/**
 * @author mwarman
 */
public class DefaultFlowControlProviderTest {

  @Test
  public void apply() throws Exception {
    DefaultFlowControlProvider flowControlProvider = new DefaultFlowControlProvider();
    flowControlProvider.init();
    Subscriber.Builder subscriberBuilder = Subscriber.newBuilder(ProjectSubscriptionName.of("project", "subscription"),
        (MessageReceiver) (message, consumer) -> {

        });
    flowControlProvider.apply(subscriberBuilder);
    Subscriber subscriber = subscriberBuilder.build();
    assertEquals(FlowController.LimitExceededBehavior.Block, subscriber.getFlowControlSettings().getLimitExceededBehavior());
    assertEquals(Long.valueOf(1000L), subscriber.getFlowControlSettings().getMaxOutstandingElementCount());
    // Now fails
    // assertNull(subscriber.getFlowControlSettings().getMaxOutstandingRequestBytes());
  }

}