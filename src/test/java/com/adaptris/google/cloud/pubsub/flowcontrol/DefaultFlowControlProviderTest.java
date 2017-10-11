package com.adaptris.google.cloud.pubsub.flowcontrol;

import com.google.api.gax.batching.FlowController;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.SubscriptionName;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * @author mwarman
 */
public class DefaultFlowControlProviderTest {

  @Test
  public void apply() throws Exception {
    DefaultFlowControlProvider flowControlProvider = new DefaultFlowControlProvider();
    flowControlProvider.init();
    Subscriber.Builder subscriberBuilder = Subscriber.defaultBuilder(SubscriptionName.create("project", "subscription"), new MessageReceiver() {
      @Override
      public void receiveMessage(PubsubMessage message, AckReplyConsumer consumer) {

      }
    });
    flowControlProvider.apply(subscriberBuilder);
    Subscriber subscriber = subscriberBuilder.build();
    assertEquals(FlowController.LimitExceededBehavior.Block, subscriber.getFlowControlSettings().getLimitExceededBehavior());
    assertNull(subscriber.getFlowControlSettings().getMaxOutstandingElementCount());
    //Following is based on calculation to do with available memory, therefore cannot do anything but assert not null.
    assertNotNull(subscriber.getFlowControlSettings().getMaxOutstandingRequestBytes());
  }

}