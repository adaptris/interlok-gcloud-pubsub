package com.adaptris.google.cloud.pubsub.flowcontrol;

import com.google.cloud.pubsub.v1.Subscriber;
import com.thoughtworks.xstream.annotations.XStreamAlias;

/**
 * @author mwarman
 */
@XStreamAlias("default-flow-control-provider")
public class DefaultFlowControlProvider extends FlowControlProvider {

  @Override
  public void apply(Subscriber.Builder subscriberBuilder){

  }
}
