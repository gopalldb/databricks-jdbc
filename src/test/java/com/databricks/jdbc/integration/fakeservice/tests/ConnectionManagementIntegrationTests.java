package com.databricks.jdbc.integration.fakeservice.tests;

import static com.databricks.jdbc.integration.IntegrationTestUtil.*;
import static org.junit.jupiter.api.Assertions.*;

import com.databricks.jdbc.integration.fakeservice.AbstractFakeServiceIntegrationTests;
import java.sql.*;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for Connection management operations. Tests cover isClosed(), isValid(),
 * catalog/schema retrieval and switching.
 */
public class ConnectionManagementIntegrationTests extends AbstractFakeServiceIntegrationTests {

  @Test
  void testIsClosed_NewConnection() throws SQLException {
    Connection conn = getValidJDBCConnection();
    assertFalse(conn.isClosed(), "Newly created connection should not be closed");

    conn.close();
    assertTrue(conn.isClosed(), "Connection should be closed after close()");
  }

  @Test
  void testIsValid_ActiveConnection() throws SQLException {
    Connection conn = getValidJDBCConnection();

    // isValid with a positive timeout should return true for an active connection
    assertTrue(conn.isValid(5), "Active connection should be valid");

    conn.close();
    assertFalse(conn.isValid(5), "Closed connection should not be valid");
  }

  @Test
  void testGetCatalog_ReturnsNonNull() throws SQLException {
    Connection conn = getValidJDBCConnection();

    String catalog = conn.getCatalog();
    assertNotNull(catalog, "getCatalog() should return non-null for active connection");
    assertFalse(catalog.isEmpty(), "getCatalog() should return non-empty string");

    conn.close();
  }

  @Test
  void testGetSchema_ReturnsNonNull() throws SQLException {
    Connection conn = getValidJDBCConnection();

    String schema = conn.getSchema();
    assertNotNull(schema, "getSchema() should return non-null for active connection");
    assertFalse(schema.isEmpty(), "getSchema() should return non-empty string");

    conn.close();
  }
}
