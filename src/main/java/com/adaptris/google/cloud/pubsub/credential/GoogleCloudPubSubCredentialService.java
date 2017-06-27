package com.adaptris.google.cloud.pubsub.credential;


import com.adaptris.annotation.AdvancedConfig;
import com.adaptris.core.*;
import com.adaptris.core.fs.FsHelper;
import com.adaptris.util.text.DateFormatUtil;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import com.thoughtworks.xstream.annotations.XStreamImplicit;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.io.File;
import java.io.FileInputStream;
import java.net.URL;
import java.util.List;

@XStreamAlias("google-cloud-pubsub-credential-service")
public class GoogleCloudPubSubCredentialService extends ServiceImp {

  public static final String DEFAULT_ACCESS_TOKEN_KEY = "access_token";
  public static final String DEFAULT_ACCESS_TOKEN_EXPIRATION_KEY = "access_token_expiration";

  @NotNull
  @Valid
  private MessageDrivenDestination jsonKeyFile;

  @NotNull
  @Valid
  @XStreamImplicit
  private List<String> scope;

  @AdvancedConfig
  private String accessTokenKey;

  @AdvancedConfig
  private String accessTokenExpirationKey;

  private transient CredentialWrapper credentialWrapper = new DefaultCredentialWrapper();

  public GoogleCloudPubSubCredentialService(){
    setAccessTokenKey(DEFAULT_ACCESS_TOKEN_KEY);
    setAccessTokenExpirationKey(DEFAULT_ACCESS_TOKEN_EXPIRATION_KEY);
  }

  public GoogleCloudPubSubCredentialService(MessageDrivenDestination destination, List<String> scope){
    this();
    setJsonKeyFile(destination);
    setScope(scope);
  }

  @Override
  public void doService(AdaptrisMessage msg) throws ServiceException {
    try {
      String destinationUrl = getJsonKeyFile().getDestination(msg);
      URL url = FsHelper.createUrlFromString(destinationUrl, true);
      File jsonKey = FsHelper.createFileReference(url);
      GoogleCredentials credential = getCredentialWrapper()
          .fromStreamWithScope(new FileInputStream(jsonKey), getScope());
      AccessToken accessToken = credential.refreshAccessToken();
      msg.addMetadata(getAccessTokenKey(), accessToken.getTokenValue());
      msg.addMetadata(getAccessTokenExpirationKey(), DateFormatUtil.format(accessToken.getExpirationTime()));
    } catch (Exception e) {
      throw new ServiceException("Failed to retrieve credentials", e);
    }
  }

  @Override
  protected void initService() throws CoreException {
    if (getJsonKeyFile() == null){
      throw new CoreException("Value for json-key-file is invalid");
    }
    if (getScope() == null){
      throw new CoreException("Value for scope is invalid");
    }
  }

  @Override
  public void prepare() throws CoreException {

  }

  @Override
  protected void closeService() {

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

  public String getAccessTokenKey() {
    return accessTokenKey;
  }

  public void setAccessTokenKey(String accessTokenKey) {
    this.accessTokenKey = accessTokenKey;
  }

  public String getAccessTokenExpirationKey() {
    return accessTokenExpirationKey;
  }

  public void setAccessTokenExpirationKey(String accessTokenExpirationKey) {
    this.accessTokenExpirationKey = accessTokenExpirationKey;
  }

  CredentialWrapper getCredentialWrapper() {
    return credentialWrapper;
  }

  void setCredentialWrapper(CredentialWrapper credentialWrapper) {
    this.credentialWrapper = credentialWrapper;
  }
}
