package com.databricks.jdbc.integration.fakeservice.tests;

import static com.databricks.jdbc.integration.IntegrationTestUtil.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

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

  // --- Catalog and schema switching tests ---

  @Test
  void testSetAndGetCatalog() throws SQLException {
    assumeTrue(isSqlExecSdkClient(), "Thrift recording not available for this test");
    Connection conn = getValidJDBCConnection();

    String originalCatalog = conn.getCatalog();
    assertNotNull(originalCatalog, "getCatalog() should return non-null");

    // Set catalog to the test catalog (which we know exists)
    String testCatalog = getDatabricksCatalog();
    conn.setCatalog(testCatalog);
    assertEquals(testCatalog, conn.getCatalog(), "getCatalog() should return what was set");

    conn.close();
  }

  @Test
  void testSetAndGetSchema() throws SQLException {
    assumeTrue(isSqlExecSdkClient(), "Thrift recording not available for this test");
    Connection conn = getValidJDBCConnection();

    String originalSchema = conn.getSchema();
    assertNotNull(originalSchema, "getSchema() should return non-null");

    // First switch to the test catalog so the test schema is accessible
    conn.setCatalog(getDatabricksCatalog());

    // Set schema to the test schema (which exists in the test catalog)
    String testSchema = getDatabricksSchema();
    conn.setSchema(testSchema);
    assertEquals(testSchema, conn.getSchema(), "getSchema() should return what was set");

    conn.close();
  }

  @Test
  void testSetAndGetClientInfo() throws SQLException {
    assumeTrue(isSqlExecSdkClient(), "Thrift recording not available for this test");
    Connection conn = getValidJDBCConnection();

    // getClientInfo() should return non-null Properties
    Properties clientInfo = conn.getClientInfo();
    assertNotNull(clientInfo, "getClientInfo() should return non-null Properties");

    // getClientInfo(name) for unknown property should return null
    String value = conn.getClientInfo("NonExistentProperty");
    assertNull(value, "getClientInfo for unknown property should return null");

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
