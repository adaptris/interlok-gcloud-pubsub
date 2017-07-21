package com.adaptris.google.cloud.pubsub.credentials;

import com.adaptris.core.CoreException;
import com.thoughtworks.xstream.annotations.XStreamImplicit;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.util.List;

public abstract class ScopedCredentialsProvider extends CredentialsProvider{

  @NotNull
  @Valid
  @XStreamImplicit(itemFieldName = "scope")
  private List<String> scopes;

  @Override
  void validateArguments() throws CoreException {
    if(getScopes() == null || getScopes().size() == 0){
      throw new CoreException("Scope is invalid");
    }
  }

  public List<String> getScopes() {
    return scopes;
  }

  public void setScopes(List<String> scopes) {
    this.scopes = scopes;
  }
}
