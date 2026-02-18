package com.databricks.jdbc.integration.fakeservice.tests;

import static com.databricks.jdbc.integration.IntegrationTestUtil.*;
import static org.junit.jupiter.api.Assertions.*;

import com.databricks.jdbc.common.DatabricksJdbcUrlParams;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.integration.fakeservice.AbstractFakeServiceIntegrationTests;
import com.databricks.jdbc.integration.fakeservice.FakeServiceConfigLoader;
import java.sql.*;
import java.util.Properties;
import org.junit.jupiter.api.Test;

/** Integration tests for connection to Databricks service. */
public class ConnectionIntegrationTests extends AbstractFakeServiceIntegrationTests {

  @Test
  void testSuccessfulConnection() throws SQLException {
    Connection conn = getValidJDBCConnection();
    assert ((conn != null) && !conn.isClosed());

    conn.close();
  }

  @Test
  void testIncorrectCredentialsForPAT() {
    Properties extraProps = new Properties();
    extraProps.put(DatabricksJdbcUrlParams.UID.getParamName(), getDatabricksUser());
    extraProps.put(DatabricksJdbcUrlParams.PASSWORD.getParamName(), "bad_token_1");
    String url = getFakeServiceJDBCUrl();
    DatabricksSQLException e =
        assertThrows(
            DatabricksSQLException.class,
            () -> DriverManager.getConnection(url, createConnectionProperties(extraProps)));

    assert e.getMessage()
        .contains("Connection failure while using the OSS Databricks JDBC driver.");
  }

  @Test
  void testIncorrectCredentialsForOAuth() {
    // SSL is disabled as embedded web server of fake service uses HTTP protocol.
    // Note that in RECORD mode, the web server interacts with production services over HTTPS.
    String template =
        "jdbc:databricks://%s/default;transportMode=http;ssl=0;AuthMech=11;AuthFlow=0;httpPath=%s";
    String url =
        String.format(
            template,
            getFakeServiceHost(),
            FakeServiceConfigLoader.getProperty(DatabricksJdbcUrlParams.HTTP_PATH.getParamName()));

    Properties extraProps = new Properties();
    extraProps.put(DatabricksJdbcUrlParams.UID.getParamName(), getDatabricksUser());
    extraProps.put(DatabricksJdbcUrlParams.PASSWORD.getParamName(), "bad_token_2");
    DatabricksSQLException e =
        assertThrows(
            DatabricksSQLException.class,
            () -> DriverManager.getConnection(url, createConnectionProperties(extraProps)));

    assert e.getMessage()
        .contains("Connection failure while using the OSS Databricks JDBC driver.");
  }

  @Test
  void testPATinOAuthTokenPassThrough() throws Exception {
    // SSL is disabled as embedded web server of fake service uses HTTP protocol.
    // Note that in RECORD mode, the web server interacts with production services over HTTPS.
    String template =
        "jdbc:databricks://%s/default;transportMode=http;ssl=0;AuthMech=11;AuthFlow=0;httpPath=%s;";
    String url =
        String.format(
            template,
            getFakeServiceHost(),
            FakeServiceConfigLoader.getProperty(DatabricksJdbcUrlParams.HTTP_PATH.getParamName()));
    Properties extraProps = new Properties();
    extraProps.put(DatabricksJdbcUrlParams.AUTH_ACCESS_TOKEN.getParamName(), getDatabricksToken());
    Connection conn = DriverManager.getConnection(url, createConnectionProperties(extraProps));
    assert ((conn != null) && !conn.isClosed());

    conn.close();
  }

  // --- Transaction and connection attribute tests ---

  @Test
  void testAutoCommit_DefaultIsTrue() throws SQLException {
    Connection conn = getValidJDBCConnection();

    assertTrue(conn.getAutoCommit(), "Default autoCommit should be true");

    conn.close();
  }

  @Test
  void testIsReadOnly_Default() throws SQLException {
    Connection conn = getValidJDBCConnection();

    boolean readOnly = conn.isReadOnly();
    assertFalse(readOnly, "Default connection should not be read-only");

    conn.close();
  }

  @Test
  void testGetTransactionIsolation() throws SQLException {
    Connection conn = getValidJDBCConnection();

    int isolation = conn.getTransactionIsolation();
    assertTrue(
        isolation == Connection.TRANSACTION_NONE
            || isolation == Connection.TRANSACTION_READ_UNCOMMITTED
            || isolation == Connection.TRANSACTION_READ_COMMITTED
            || isolation == Connection.TRANSACTION_REPEATABLE_READ
            || isolation == Connection.TRANSACTION_SERIALIZABLE,
        "Transaction isolation should be a valid JDBC constant");

    conn.close();
  }

  private Properties createConnectionProperties(Properties extraProps) {
    Properties connProps = new Properties();
    connProps.putAll(extraProps);
    connProps.put(
        DatabricksJdbcUrlParams.CONN_CATALOG.getParamName(),
        FakeServiceConfigLoader.getProperty(DatabricksJdbcUrlParams.CONN_CATALOG.getParamName()));
    connProps.put(
        DatabricksJdbcUrlParams.CONN_SCHEMA.getParamName(),
        FakeServiceConfigLoader.getProperty(DatabricksJdbcUrlParams.CONN_SCHEMA.getParamName()));
    connProps.put(
        DatabricksJdbcUrlParams.USE_THRIFT_CLIENT.getParamName(),
        FakeServiceConfigLoader.shouldUseThriftClient());

    return connProps;
  }
}
