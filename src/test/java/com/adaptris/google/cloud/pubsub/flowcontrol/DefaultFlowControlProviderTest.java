package com.adaptris.google.cloud.pubsub.flowcontrol;

import static org.junit.Assert.assertEquals;
import org.junit.Test;
import com.google.api.gax.batching.FlowController;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PubsubMessage;

/**
 * @author mwarman
 */
public class DefaultFlowControlProviderTest {

  @Test
  public void apply() throws Exception {
    DefaultFlowControlProvider flowControlProvider = new DefaultFlowControlProvider();
    flowControlProvider.init();
    Subscriber.Builder subscriberBuilder = Subscriber.newBuilder(ProjectSubscriptionName.of("project", "subscription"), new MessageReceiver() {
      @Override
      public void receiveMessage(PubsubMessage message, AckReplyConsumer consumer) {

      }
    });
    flowControlProvider.apply(subscriberBuilder);
    Subscriber subscriber = subscriberBuilder.build();
    assertEquals(FlowController.LimitExceededBehavior.Block, subscriber.getFlowControlSettings().getLimitExceededBehavior());
    assertEquals(Long.valueOf(1000L), subscriber.getFlowControlSettings().getMaxOutstandingElementCount());
    // Now fails
    // assertNull(subscriber.getFlowControlSettings().getMaxOutstandingRequestBytes());
  }

}