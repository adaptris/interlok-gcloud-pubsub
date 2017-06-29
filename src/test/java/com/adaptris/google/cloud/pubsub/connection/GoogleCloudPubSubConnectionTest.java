package com.adaptris.google.cloud.pubsub.connection;

import com.adaptris.core.CoreException;
import com.adaptris.google.cloud.pubsub.consumer.GoogleCloudPubSubPullConsumer;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;

import static org.junit.Assert.*;

public class GoogleCloudPubSubConnectionTest {

  @Test
  public void testPrepareConnection() throws Exception {
    GoogleCloudPubSubConnection connection = new GoogleCloudPubSubConnection();
    prepareFail(connection, "Project Name is invalid");
    connection.setProjectName("");
    prepareFail(connection, "Project Name is invalid");
    connection.setProjectName("project-name");
    prepareFail(connection, "Json Key File is invalid");
    connection.setJsonKeyFile("");
    prepareFail(connection, "Json Key File is invalid");
    connection.setJsonKeyFile("/opt/interlok/file.json");
    prepareFail(connection, "Scope is invalid");
    connection.setScopes(new ArrayList<String>());
    prepareFail(connection, "Scope is invalid");
    connection.setScopes(Arrays.asList("scope"));
    connection.prepareConnection();
  }

  private void prepareFail(GoogleCloudPubSubConnection connection, String message){
    try {
      connection.prepareConnection();
      fail();
    } catch (CoreException expected){
      assertEquals(message, expected.getMessage());
    }
  }

  @Test
  public void testGetProjectName() throws Exception {
    GoogleCloudPubSubConnection connection = new GoogleCloudPubSubConnection();
    connection.setProjectName("project-name");
    assertEquals("project-name", connection.getProjectName());
  }

  @Test
  public void testGetJsonKeyFile() throws Exception {
    GoogleCloudPubSubConnection connection = new GoogleCloudPubSubConnection();
    connection.setJsonKeyFile("/opt/interlok/file.json");
    assertEquals("/opt/interlok/file.json", connection.getJsonKeyFile());
  }

  @Test
  public void testGetScopes() throws Exception {
    GoogleCloudPubSubConnection connection = new GoogleCloudPubSubConnection();
    connection.setScopes(Arrays.asList("scope"));
    assertEquals(1, connection.getScopes().size());
    assertEquals("scope", connection.getScopes().get(0));
  }

}