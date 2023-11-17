package com.adaptris.google.cloud.pubsub.channel;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

import org.apache.commons.lang3.StringUtils;

import com.adaptris.annotation.InputFieldDefault;
import com.adaptris.core.CoreException;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.thoughtworks.xstream.annotations.XStreamAlias;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

/**
 * @config custom-channel-provider
 */
@XStreamAlias("custom-channel-provider")
public class CustomChannelProvider extends ChannelProvider {

  @Valid
  @NotNull
  private String address;

  @Valid
  @NotNull
  @InputFieldDefault(value = "true")
  private Boolean usePlaintext = true;

  public CustomChannelProvider() {
  }

  public CustomChannelProvider(String address) {
    this();
    setAddress(address);
  }

  public CustomChannelProvider(String address, Boolean usePlaintext) {
    this(address);
    setUsePlaintext(usePlaintext);
  }

  @Override
  void validateArguments() throws CoreException {
    if (StringUtils.isEmpty(getAddress())) {
      throw new CoreException("Address is invalid");
    }
    if (getUsePlaintext() == null) {
      throw new CoreException("Use Plaintext is invalid");
    }
  }

  @Override
  TransportChannelProvider createChannelProvider() {
    ManagedChannelBuilder<?> channelBuilder = ManagedChannelBuilder.forTarget(getAddress()).maxInboundMessageSize(MAX_INBOUND_MESSAGE_SIZE);
    if (getUsePlaintext()) {
      channelBuilder.usePlaintext();
    }
    ManagedChannel channel = channelBuilder.build();
    return FixedTransportChannelProvider.create(GrpcTransportChannel.create(channel));
  }

  public String getAddress() {
    return address;
  }

  public void setAddress(String address) {
    this.address = address;
  }

  public Boolean getUsePlaintext() {
    return usePlaintext;
  }

  public void setUsePlaintext(Boolean usePlaintext) {
    this.usePlaintext = usePlaintext;
  }

}
