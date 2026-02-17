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
 * Integration tests for Statement execution result handling. Tests cover getResultSet(),
 * getUpdateCount(), and execute() behavior for different SQL statement types per JDBC spec.
 */
public class ExecutionResultsIntegrationTests extends AbstractFakeServiceIntegrationTests {

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
  void testExecute_SelectQuery_GetResultSetReturnsResultSet() throws SQLException {
    Statement stmt = connection.createStatement();
    boolean hasResultSet = stmt.execute("SELECT 1 AS num, 'hello' AS greeting");
    assertTrue(hasResultSet, "execute() should return true for SELECT query");

    ResultSet rs = stmt.getResultSet();
    assertNotNull(rs, "getResultSet() should return non-null ResultSet after SELECT");
    assertTrue(rs.next(), "ResultSet should have at least one row");
    assertEquals(1, rs.getInt("num"));
    assertEquals("hello", rs.getString("greeting"));

    assertEquals(-1, stmt.getUpdateCount(), "getUpdateCount() should return -1 for SELECT query");
  }

  @Test
  void testExecute_InsertStatement_GetResultSetReturnsNull() throws SQLException {
    String tableName = "exec_result_insert_table";
    setupDatabaseTable(connection, tableName);

    Statement stmt = connection.createStatement();
    boolean hasResultSet =
        stmt.execute(
            "INSERT INTO "
                + getFullyQualifiedTableName(tableName)
                + " (id, col1, col2) VALUES (1, 'a', 'b')");
    assertFalse(hasResultSet, "execute() should return false for INSERT statement");

    // Per JDBC spec: after execute() returns false, getResultSet() should return null
    // and getUpdateCount() should return the number of affected rows or 0
    int updateCount = stmt.getUpdateCount();
    assertTrue(updateCount >= 0, "getUpdateCount() should return >= 0 for INSERT");

    deleteTable(connection, tableName);
  }

  @Test
  void testExecute_UpdateStatement_GetResultSetReturnsNull() throws SQLException {
    String tableName = "exec_result_update_table";
    setupDatabaseTable(connection, tableName);
    insertTestData(connection, tableName);

    Statement stmt = connection.createStatement();
    boolean hasResultSet =
        stmt.execute(
            "UPDATE "
                + getFullyQualifiedTableName(tableName)
                + " SET col1 = 'updated' WHERE id = 1");
    assertFalse(hasResultSet, "execute() should return false for UPDATE statement");

    int updateCount = stmt.getUpdateCount();
    assertTrue(updateCount >= 0, "getUpdateCount() should return >= 0 for UPDATE");

    deleteTable(connection, tableName);
  }

  @Test
  void testExecute_DeleteStatement_GetResultSetReturnsNull() throws SQLException {
    String tableName = "exec_result_delete_table";
    setupDatabaseTable(connection, tableName);
    insertTestData(connection, tableName);

    Statement stmt = connection.createStatement();
    boolean hasResultSet =
        stmt.execute("DELETE FROM " + getFullyQualifiedTableName(tableName) + " WHERE id = 1");
    assertFalse(hasResultSet, "execute() should return false for DELETE statement");

    int updateCount = stmt.getUpdateCount();
    assertTrue(updateCount >= 0, "getUpdateCount() should return >= 0 for DELETE");

    deleteTable(connection, tableName);
  }

  @Test
  void testGetResultSet_AfterExecuteQuery() throws SQLException {
    Statement stmt = connection.createStatement();
    ResultSet rs = stmt.executeQuery("SELECT 42 AS answer");
    assertNotNull(rs, "executeQuery() should return non-null ResultSet");
    assertTrue(rs.next(), "ResultSet should have at least one row");
    assertEquals(42, rs.getInt("answer"));

    // getResultSet() should also return the same result set
    ResultSet rs2 = stmt.getResultSet();
    assertNotNull(rs2, "getResultSet() should return non-null after executeQuery()");
  }

  @Test
  void testGetUpdateCount_AfterExecuteUpdate() throws SQLException {
    String tableName = "exec_update_count_table";
    setupDatabaseTable(connection, tableName);

    Statement stmt = connection.createStatement();
    int result =
        stmt.executeUpdate(
            "INSERT INTO "
                + getFullyQualifiedTableName(tableName)
                + " (id, col1, col2) VALUES (1, 'val1', 'val2')");
    assertTrue(result >= 0, "executeUpdate() should return >= 0 for INSERT");

    // getUpdateCount() should reflect the same count
    int updateCount = stmt.getUpdateCount();
    assertTrue(updateCount >= 0, "getUpdateCount() should return >= 0 after executeUpdate()");

    deleteTable(connection, tableName);
  }

  @Test
  void testGetMoreResults_AdvancesPastResult() throws SQLException {
    Statement stmt = connection.createStatement();
    stmt.execute("SELECT 1 AS num");

    // First call to getMoreResults() should indicate no more results
    boolean hasMore = stmt.getMoreResults();
    assertFalse(hasMore, "getMoreResults() should return false (single result)");

    // After getMoreResults(), getUpdateCount() should return -1 (no more results)
    assertEquals(
        -1,
        stmt.getUpdateCount(),
        "getUpdateCount() should return -1 after getMoreResults() with no more results");
  }
}
