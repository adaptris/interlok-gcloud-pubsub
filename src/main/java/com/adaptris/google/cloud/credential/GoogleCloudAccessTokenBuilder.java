package com.adaptris.google.cloud.credential;


import com.adaptris.core.*;
import com.adaptris.core.fs.FsHelper;
import com.adaptris.core.http.oauth.AccessToken;
import com.adaptris.core.http.oauth.AccessTokenBuilder;
import com.google.auth.oauth2.GoogleCredentials;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import com.thoughtworks.xstream.annotations.XStreamImplicit;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URL;
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

  private transient CredentialWrapper credentialWrapper = new DefaultCredentialWrapper();

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
      String destinationUrl = getJsonKeyFile().getDestination(adaptrisMessage);
      URL url = FsHelper.createUrlFromString(destinationUrl, true);
      File jsonKey = FsHelper.createFileReference(url);
      GoogleCredentials credential = getCredentialWrapper()
          .fromStreamWithScope(new FileInputStream(jsonKey), getScope());
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

  CredentialWrapper getCredentialWrapper() {
    return credentialWrapper;
  }

  void setCredentialWrapper(CredentialWrapper credentialWrapper) {
    this.credentialWrapper = credentialWrapper;
  }
}
