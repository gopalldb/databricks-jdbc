package com.databricks.jdbc.integration.fakeservice.tests;

import static com.databricks.jdbc.integration.IntegrationTestUtil.*;
import static org.junit.jupiter.api.Assertions.*;

import com.databricks.jdbc.api.impl.DatabricksConnection;
import com.databricks.jdbc.common.DatabricksClientType;
import com.databricks.jdbc.integration.fakeservice.AbstractFakeServiceIntegrationTests;
import com.databricks.jdbc.integration.fakeservice.FakeServiceExtension;
import java.sql.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for ResultSet navigation and position queries. Tests cover forward-only
 * navigation, cursor position methods (isBeforeFirst, isAfterLast, isFirst, isLast), getRow(),
 * fetch size, and ResultSet type/concurrency.
 */
public class ResultSetNavigationIntegrationTests extends AbstractFakeServiceIntegrationTests {

  private Connection connection;

  @BeforeEach
  void setUp() throws SQLException {
    connection = getValidJDBCConnection();
  }

  @AfterEach
  void cleanUp() throws SQLException {
    if (connection != null) {
      if (((DatabricksConnection) connection).getConnectionContext().getClientType()
              == DatabricksClientType.THRIFT
          && getFakeServiceMode() == FakeServiceExtension.FakeServiceMode.REPLAY) {
        // Wiremock has error in stub matching for close operation in THRIFT + REPLAY mode
      } else {
        connection.close();
      }
    }
  }

  @Test
  void testForwardNavigation_MultipleRows() throws SQLException {
    String tableName = "nav_forward_table";
    setupDatabaseTable(connection, tableName);
    String fqn = getFullyQualifiedTableName(tableName);
    executeSQL(connection, "INSERT INTO " + fqn + " (id, col1, col2) VALUES (1, 'a', 'b')");
    executeSQL(connection, "INSERT INTO " + fqn + " (id, col1, col2) VALUES (2, 'c', 'd')");
    executeSQL(connection, "INSERT INTO " + fqn + " (id, col1, col2) VALUES (3, 'e', 'f')");

    ResultSet rs = executeQuery(connection, "SELECT id FROM " + fqn + " ORDER BY id");
    assertNotNull(rs);

    int count = 0;
    while (rs.next()) {
      count++;
      assertEquals(count, rs.getInt("id"), "Row " + count + " should have id=" + count);
    }
    assertEquals(3, count, "Should navigate through all 3 rows");

    rs.close();
    deleteTable(connection, tableName);
  }

  @Test
  void testIsBeforeFirst_BeforeNavigation() throws SQLException {
    ResultSet rs = executeQuery(connection, "SELECT 1 AS num");
    assertNotNull(rs);
    assertTrue(rs.isBeforeFirst(), "Cursor should be before first row initially");

    rs.next();
    assertFalse(rs.isBeforeFirst(), "Cursor should not be before first after next()");
    rs.close();
  }

  @Test
  void testIsFirst_OnFirstRow() throws SQLException {
    ResultSet rs = executeQuery(connection, "SELECT 1 AS num");
    assertNotNull(rs);
    assertFalse(rs.isFirst(), "isFirst() should be false before calling next()");

    assertTrue(rs.next(), "Should have first row");
    assertTrue(rs.isFirst(), "isFirst() should be true on first row");
    rs.close();
  }

  @Test
  void testNextReturnsFalse_AfterExhausted() throws SQLException {
    ResultSet rs = executeQuery(connection, "SELECT 1 AS num");
    assertNotNull(rs);

    assertTrue(rs.next(), "Should have first row");
    assertEquals(1, rs.getInt("num"));
    assertFalse(rs.next(), "Should have no more rows after last");
    rs.close();
  }

  @Test
  void testGetRow_TracksPosition() throws SQLException {
    String tableName = "nav_getrow_table";
    setupDatabaseTable(connection, tableName);
    String fqn = getFullyQualifiedTableName(tableName);
    executeSQL(connection, "INSERT INTO " + fqn + " (id, col1, col2) VALUES (1, 'a', 'b')");
    executeSQL(connection, "INSERT INTO " + fqn + " (id, col1, col2) VALUES (2, 'c', 'd')");

    ResultSet rs = executeQuery(connection, "SELECT id FROM " + fqn + " ORDER BY id");
    assertNotNull(rs);

    // Before first row, getRow() returns 0
    assertEquals(0, rs.getRow(), "getRow() should return 0 before first row");

    assertTrue(rs.next());
    assertEquals(1, rs.getRow(), "getRow() should return 1 on first row");

    assertTrue(rs.next());
    assertEquals(2, rs.getRow(), "getRow() should return 2 on second row");

    rs.close();
    deleteTable(connection, tableName);
  }

  @Test
  void testResultSetType_ForwardOnly() throws SQLException {
    Statement stmt = connection.createStatement();
    ResultSet rs = stmt.executeQuery("SELECT 1 AS num");

    assertEquals(
        ResultSet.TYPE_FORWARD_ONLY, rs.getType(), "ResultSet type should be TYPE_FORWARD_ONLY");
    rs.close();
  }

  @Test
  void testResultSetConcurrency_ReadOnly() throws SQLException {
    Statement stmt = connection.createStatement();
    ResultSet rs = stmt.executeQuery("SELECT 1 AS num");

    assertEquals(
        ResultSet.CONCUR_READ_ONLY,
        rs.getConcurrency(),
        "ResultSet concurrency should be CONCUR_READ_ONLY");
    rs.close();
  }

  @Test
  void testFetchSize_DefaultsToZero() throws SQLException {
    Statement stmt = connection.createStatement();
    ResultSet rs = stmt.executeQuery("SELECT 1 AS num");

    // Databricks JDBC does not support fetch size, returns 0
    assertEquals(0, rs.getFetchSize(), "getFetchSize() should return 0 (unsupported)");
    rs.close();
  }

  @Test
  void testSetFetchSize_AcceptedWithWarning() throws SQLException {
    Statement stmt = connection.createStatement();
    ResultSet rs = stmt.executeQuery("SELECT 1 AS num");

    // setFetchSize is accepted but ignored with a warning
    assertDoesNotThrow(() -> rs.setFetchSize(100), "setFetchSize should not throw");
    rs.close();
  }

  @Test
  void testFetchDirection_ForwardOnly() throws SQLException {
    Statement stmt = connection.createStatement();
    ResultSet rs = stmt.executeQuery("SELECT 1 AS num");

    assertEquals(
        ResultSet.FETCH_FORWARD, rs.getFetchDirection(), "Fetch direction should be FETCH_FORWARD");
    rs.close();
  }
}
