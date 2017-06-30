package com.adaptris.google.cloud.pubsub.connection.channel;

import com.adaptris.annotation.InputFieldDefault;
import com.adaptris.core.CoreException;
import com.google.api.gax.grpc.FixedChannelProvider;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.apache.commons.lang.StringUtils;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

@XStreamAlias("custom-channel-provider")
public class CustomChannelProvider extends ChannelProvider {

  @Valid
  @NotNull
  private String address;

  @Valid
  @NotNull
  @InputFieldDefault(value = "true")
  private Boolean usePlaintext = true;

  public CustomChannelProvider(){
  }

  public CustomChannelProvider(String address){
    this();
    setAddress(address);
  }

  public CustomChannelProvider(String address, Boolean usePlaintext){
    this(address);
    setUsePlaintext(usePlaintext);
  }

  @Override
  void validateArguments() throws CoreException {
    if (StringUtils.isEmpty(getAddress())){
      throw new CoreException("Address is invalid");
    }
    if (getUsePlaintext() == null){
      throw new CoreException("Use Plaintext is invalid");
    }
  }

  @Override
  com.google.api.gax.grpc.ChannelProvider createChannelProvider() {
    ManagedChannel channel = ManagedChannelBuilder.forTarget(getAddress()).usePlaintext(getUsePlaintext()).build();
    return FixedChannelProvider.create(channel);
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
