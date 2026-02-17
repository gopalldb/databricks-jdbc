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
 * Integration tests for Statement and PreparedStatement batch operations. Tests cover addBatch(),
 * executeBatch(), clearBatch(), and edge cases like empty batch execution.
 */
public class BatchExecutionIntegrationTests extends AbstractFakeServiceIntegrationTests {

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
  void testStatementBatch_MultipleInserts() throws SQLException {
    String tableName = "batch_multi_insert_table";
    setupDatabaseTable(connection, tableName);

    Statement stmt = connection.createStatement();
    String fqn = getFullyQualifiedTableName(tableName);
    stmt.addBatch("INSERT INTO " + fqn + " (id, col1, col2) VALUES (1, 'a1', 'b1')");
    stmt.addBatch("INSERT INTO " + fqn + " (id, col1, col2) VALUES (2, 'a2', 'b2')");
    stmt.addBatch("INSERT INTO " + fqn + " (id, col1, col2) VALUES (3, 'a3', 'b3')");

    int[] updateCounts = stmt.executeBatch();
    assertEquals(3, updateCounts.length, "Should have 3 update counts");

    // Verify all rows were inserted
    ResultSet rs = executeQuery(connection, "SELECT COUNT(*) AS cnt FROM " + fqn);
    assertTrue(rs.next());
    assertEquals(3, rs.getInt("cnt"), "All 3 rows should be inserted");

    deleteTable(connection, tableName);
  }

  @Test
  void testStatementBatch_MixedDMLOperations() throws SQLException {
    String tableName = "batch_mixed_dml_table";
    setupDatabaseTable(connection, tableName);
    insertTestData(connection, tableName);

    Statement stmt = connection.createStatement();
    String fqn = getFullyQualifiedTableName(tableName);
    stmt.addBatch("INSERT INTO " + fqn + " (id, col1, col2) VALUES (2, 'x', 'y')");
    stmt.addBatch("UPDATE " + fqn + " SET col1 = 'updated' WHERE id = 1");

    int[] updateCounts = stmt.executeBatch();
    assertEquals(2, updateCounts.length, "Should have 2 update counts");

    // Verify the insert
    ResultSet rs = executeQuery(connection, "SELECT COUNT(*) AS cnt FROM " + fqn);
    assertTrue(rs.next());
    assertEquals(2, rs.getInt("cnt"), "Should have 2 rows total");

    // Verify the update
    ResultSet rs2 = executeQuery(connection, "SELECT col1 FROM " + fqn + " WHERE id = 1");
    assertTrue(rs2.next());
    assertEquals("updated", rs2.getString("col1"));

    deleteTable(connection, tableName);
  }

  @Test
  void testStatementBatch_ClearBatch() throws SQLException {
    String tableName = "batch_clear_table";
    setupDatabaseTable(connection, tableName);

    Statement stmt = connection.createStatement();
    String fqn = getFullyQualifiedTableName(tableName);
    stmt.addBatch("INSERT INTO " + fqn + " (id, col1, col2) VALUES (1, 'a', 'b')");
    stmt.addBatch("INSERT INTO " + fqn + " (id, col1, col2) VALUES (2, 'c', 'd')");

    // Clear the batch - no commands should execute
    stmt.clearBatch();

    int[] updateCounts = stmt.executeBatch();
    assertEquals(0, updateCounts.length, "Empty batch should return empty array");

    // Verify no rows were inserted
    ResultSet rs = executeQuery(connection, "SELECT COUNT(*) AS cnt FROM " + fqn);
    assertTrue(rs.next());
    assertEquals(0, rs.getInt("cnt"), "No rows should be inserted after clearBatch");

    deleteTable(connection, tableName);
  }

  @Test
  void testStatementBatch_EmptyBatch() throws SQLException {
    Statement stmt = connection.createStatement();
    int[] updateCounts = stmt.executeBatch();
    assertEquals(0, updateCounts.length, "Empty batch should return empty array");
  }

  @Test
  void testPreparedStatementBatch_MultipleInserts() throws SQLException {
    String tableName = "ps_batch_insert_table";
    setupDatabaseTable(connection, tableName);

    String insertSQL =
        "INSERT INTO "
            + getFullyQualifiedTableName(tableName)
            + " (id, col1, col2) VALUES (?, ?, ?)";
    PreparedStatement pstmt = connection.prepareStatement(insertSQL);

    pstmt.setInt(1, 1);
    pstmt.setString(2, "val1");
    pstmt.setString(3, "val2");
    pstmt.addBatch();

    pstmt.setInt(1, 2);
    pstmt.setString(2, "val3");
    pstmt.setString(3, "val4");
    pstmt.addBatch();

    int[] updateCounts = pstmt.executeBatch();
    assertEquals(2, updateCounts.length, "Should have 2 update counts");

    // Verify rows were inserted
    ResultSet rs =
        executeQuery(
            connection, "SELECT COUNT(*) AS cnt FROM " + getFullyQualifiedTableName(tableName));
    assertTrue(rs.next());
    assertEquals(2, rs.getInt("cnt"), "Both rows should be inserted");

    deleteTable(connection, tableName);
  }

  @Test
  void testPreparedStatementBatch_ClearBatch() throws SQLException {
    String tableName = "ps_batch_clear_table";
    setupDatabaseTable(connection, tableName);

    String insertSQL =
        "INSERT INTO "
            + getFullyQualifiedTableName(tableName)
            + " (id, col1, col2) VALUES (?, ?, ?)";
    PreparedStatement pstmt = connection.prepareStatement(insertSQL);

    pstmt.setInt(1, 1);
    pstmt.setString(2, "val1");
    pstmt.setString(3, "val2");
    pstmt.addBatch();

    // Clear the batch
    pstmt.clearBatch();

    // Execute with fresh parameters
    pstmt.setInt(1, 10);
    pstmt.setString(2, "fresh1");
    pstmt.setString(3, "fresh2");
    pstmt.addBatch();

    int[] updateCounts = pstmt.executeBatch();
    assertEquals(1, updateCounts.length, "Should have 1 update count after clear + re-add");

    // Verify only the fresh row was inserted
    ResultSet rs =
        executeQuery(connection, "SELECT id, col1 FROM " + getFullyQualifiedTableName(tableName));
    assertTrue(rs.next());
    assertEquals(10, rs.getInt("id"));
    assertEquals("fresh1", rs.getString("col1"));
    assertFalse(rs.next(), "Should only have 1 row");

    deleteTable(connection, tableName);
  }
}
