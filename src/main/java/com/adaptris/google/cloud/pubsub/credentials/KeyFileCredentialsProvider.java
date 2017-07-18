package com.adaptris.google.cloud.pubsub.credentials;

import com.adaptris.core.CoreException;
import com.adaptris.google.cloud.credential.CredentialBuilder;
import com.adaptris.google.cloud.credential.GoogleCredentialBuilder;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import com.thoughtworks.xstream.annotations.XStreamImplicit;
import org.apache.commons.lang.StringUtils;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.util.List;

@XStreamAlias("key-file-credentials-provider")
public class KeyFileCredentialsProvider extends CredentialsProvider{

  @NotNull
  @Valid
  private String jsonKeyFile;

  @NotNull
  @Valid
  @XStreamImplicit(itemFieldName = "scope")
  private List<String> scopes;

  private transient CredentialBuilder credentialBuilder = new GoogleCredentialBuilder();

  @Override
  void validateArguments() throws CoreException {
    if (StringUtils.isEmpty(getJsonKeyFile())){
      throw new CoreException("Json Key File is invalid");
    }
    if(getScopes() == null || getScopes().size() == 0){
      throw new CoreException("Scope is invalid");
    }
  }

  @Override
  com.google.api.gax.core.CredentialsProvider createCredentialsProvider() throws CoreException{
    return FixedCredentialsProvider.create(credentialBuilder.fromStreamWithScope(getJsonKeyFile(), getScopes()));
  }

  public String getJsonKeyFile() {
    return jsonKeyFile;
  }

  public void setJsonKeyFile(String jsonKeyFile) {
    this.jsonKeyFile = jsonKeyFile;
  }

  public List<String> getScopes() {
    return scopes;
  }

  public void setScopes(List<String> scopes) {
    this.scopes = scopes;
  }
}
