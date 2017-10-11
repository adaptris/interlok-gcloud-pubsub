package com.adaptris.google.cloud.pubsub.flowcontrol;

import com.adaptris.core.ComponentLifecycle;
import com.adaptris.core.CoreException;
import com.google.cloud.pubsub.v1.Subscriber;
import com.thoughtworks.xstream.annotations.XStreamAlias;

/**
 * @author mwarman
 */
public abstract class FlowControlProvider implements ComponentLifecycle {

  public abstract void apply(Subscriber.Builder subscriberBuilder);

  @Override
  public void init() throws CoreException {

  }

  @Override
  public void start() throws CoreException {

  }

  @Override
  public void stop() {

  }

  @Override
  public void close() {

  }
}
