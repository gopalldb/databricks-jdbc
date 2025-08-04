package com.databricks.jdbc.integration.e2e;

import static com.databricks.jdbc.common.DatabricksJdbcUrlParams.PASSWORD;
import static com.databricks.jdbc.common.DatabricksJdbcUrlParams.UID;
import static com.databricks.jdbc.integration.IntegrationTestUtil.*;
import static org.junit.jupiter.api.Assertions.*;

import java.sql.*;
import java.util.Properties;
import org.junit.jupiter.api.Test;

public class OAuthTests {
  @Test
  void testM2M() throws SQLException {
    Connection connection = getValidM2MConnection();
    assertDoesNotThrow(() -> connection.createStatement().execute("select 1"));
  }

  @Test
  void testPAT() throws SQLException {
    Properties connectionProperties = new Properties();
    connectionProperties.put(UID, getDatabricksUser());
    connectionProperties.put(PASSWORD, getDatabricksToken());
    Connection connection = DriverManager.getConnection(getJDBCUrl(), connectionProperties);
    assertDoesNotThrow(() -> connection.createStatement().execute("select 1"));
  }

  @Test
  void testSPTokenFederation() throws SQLException {
    Connection connection = getValidSPTokenFedConnection();
    assertDoesNotThrow(() -> connection.createStatement().execute("select 1"));
  }
}
