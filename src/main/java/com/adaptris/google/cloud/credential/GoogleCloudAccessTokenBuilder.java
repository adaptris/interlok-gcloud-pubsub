package com.adaptris.google.cloud.credential;


import com.adaptris.core.*;
import com.adaptris.core.http.oauth.AccessToken;
import com.adaptris.core.http.oauth.AccessTokenBuilder;
import com.google.auth.oauth2.GoogleCredentials;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import com.thoughtworks.xstream.annotations.XStreamImplicit;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.util.List;

@XStreamAlias("google-cloud-access-token-builder")
public class GoogleCloudAccessTokenBuilder implements AccessTokenBuilder {

  @NotNull
  @Valid
  private MessageDrivenDestination jsonKeyFile;

  @NotNull
  @Valid
  @XStreamImplicit(itemFieldName = "scope")
  private List<String> scope;

  private transient CredentialBuilder credentialBuilder = new GoogleCredentialBuilder();

  public GoogleCloudAccessTokenBuilder(){
  }

  public GoogleCloudAccessTokenBuilder(MessageDrivenDestination destination, List<String> scope){
    this();
    setJsonKeyFile(destination);
    setScope(scope);
  }

  @Override
  public AccessToken build(AdaptrisMessage adaptrisMessage) throws IOException, CoreException {
    try {
      GoogleCredentials credential = getCredentialBuilder()
          .fromStreamWithScope(getJsonKeyFile().getDestination(adaptrisMessage), getScope());
      com.google.auth.oauth2.AccessToken accessToken = credential.refreshAccessToken();
      return new AccessToken(accessToken.getTokenValue(), accessToken.getExpirationTime().getTime());
    } catch (Exception e) {
      throw new ServiceException("Failed to retrieve credentials", e);
    }
  }

  @Override
  public void init() throws CoreException {
    if (getJsonKeyFile() == null){
      throw new CoreException("Value for json-key-file is invalid");
    }
    if (getScope() == null){
      throw new CoreException("Value for scope is invalid");
    }
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

  public MessageDrivenDestination getJsonKeyFile() {
    return jsonKeyFile;
  }

  public void setJsonKeyFile(MessageDrivenDestination jsonKeyFile) {
    this.jsonKeyFile = jsonKeyFile;
  }

  public List<String> getScope() {
    return scope;
  }

  public void setScope(List<String> scope) {
    this.scope = scope;
  }

  CredentialBuilder getCredentialBuilder() {
    return credentialBuilder;
  }

  void setCredentialBuilder(CredentialBuilder credentialBuilder) {
    this.credentialBuilder = credentialBuilder;
  }
}
