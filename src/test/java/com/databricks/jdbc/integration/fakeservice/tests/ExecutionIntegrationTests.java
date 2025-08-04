package com.databricks.jdbc.integration.fakeservice.tests;

import static com.databricks.jdbc.dbclient.impl.sqlexec.PathConstants.SESSION_PATH;
import static com.databricks.jdbc.dbclient.impl.sqlexec.PathConstants.STATEMENT_PATH;
import static com.databricks.jdbc.integration.IntegrationTestUtil.*;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static org.junit.jupiter.api.Assertions.*;

import com.databricks.jdbc.api.IDatabricksConnection;
import com.databricks.jdbc.api.IDatabricksResultSet;
import com.databricks.jdbc.api.IDatabricksStatement;
import com.databricks.jdbc.api.impl.DatabricksConnection;
import com.databricks.jdbc.common.DatabricksClientType;
import com.databricks.jdbc.integration.fakeservice.AbstractFakeServiceIntegrationTests;
import com.databricks.jdbc.integration.fakeservice.FakeServiceExtension;
import com.databricks.sdk.service.sql.StatementState;
import com.github.tomakehurst.wiremock.client.CountMatchingStrategy;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Integration tests for SQL statement execution. */
public class ExecutionIntegrationTests extends AbstractFakeServiceIntegrationTests {

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
        // Hacky fix
        // Wiremock has error in stub matching for close operation in THRIFT + REPLAY mode
      } else {
        connection.close();
      }
    }
  }

  @Test
  void testInsertStatement() throws SQLException {
    String tableName = "insert_test_table";
    setupDatabaseTable(connection, tableName);
    String SQL =
        "INSERT INTO "
            + getFullyQualifiedTableName(tableName)
            + " (id, col1, col2) VALUES (1, 'value1', 'value2')";
    assertDoesNotThrow(() -> executeSQL(connection, SQL), "Error executing SQL");

    ResultSet rs =
        executeQuery(connection, "SELECT * FROM " + getFullyQualifiedTableName(tableName));
    int rows = 0;
    while (rs != null && rs.next()) {
      rows++;
    }
    assertEquals(1, rows, "Expected 1 row, got " + rows);
    deleteTable(connection, tableName);

    if (isSqlExecSdkClient()) {
      // Run validations for SQL_EXEC fake service
      getDatabricksApiExtension().verify(1, postRequestedFor(urlEqualTo(SESSION_PATH)));

      // At least 5 statement requests are sent: drop, create, insert, select, drop
      // There can be more for retries
      getDatabricksApiExtension()
          .verify(
              new CountMatchingStrategy(CountMatchingStrategy.GREATER_THAN_OR_EQUAL, 5),
              postRequestedFor(urlEqualTo(STATEMENT_PATH)));
    }
  }

  @Test
  void testUpdateStatement() throws SQLException {
    // Insert initial test data
    String tableName = "update_test_table";
    setupDatabaseTable(connection, tableName);
    insertTestData(connection, tableName);

    String updateSQL =
        "UPDATE "
            + getFullyQualifiedTableName(tableName)
            + " SET col1 = 'updatedValue1' WHERE id = 1";
    executeSQL(connection, updateSQL);

    ResultSet rs =
        executeQuery(
            connection,
            "SELECT col1 FROM " + getFullyQualifiedTableName(tableName) + " WHERE id = 1");
    assertTrue(
        rs.next() && "updatedValue1".equals(rs.getString("col1")),
        "Expected 'updatedValue1', got " + rs.getString("col1"));
    deleteTable(connection, tableName);

    if (isSqlExecSdkClient()) {
      // Run validations for SQL_EXEC fake service
      getDatabricksApiExtension().verify(1, postRequestedFor(urlEqualTo(SESSION_PATH)));

      // At least 6 statement requests are sent: drop, create, insert, update, select, drop
      // There can be more for retries
      getDatabricksApiExtension()
          .verify(
              new CountMatchingStrategy(CountMatchingStrategy.GREATER_THAN_OR_EQUAL, 6),
              postRequestedFor(urlEqualTo(STATEMENT_PATH)));
    }
  }

  @Test
  void testDeleteStatement() throws SQLException {
    // Insert initial test data
    String tableName = "delete_test_table";
    setupDatabaseTable(connection, tableName);
    insertTestData(connection, tableName);

    String deleteSQL = "DELETE FROM " + getFullyQualifiedTableName(tableName) + " WHERE id = 1";
    executeSQL(connection, deleteSQL);

    ResultSet rs =
        executeQuery(connection, "SELECT * FROM " + getFullyQualifiedTableName(tableName));
    assertFalse(rs.next(), "Expected no rows after delete");
    deleteTable(connection, tableName);

    if (isSqlExecSdkClient()) {
      // At least 6 statement requests are sent: drop, create, insert, delete, select, drop
      getDatabricksApiExtension()
          .verify(
              new CountMatchingStrategy(CountMatchingStrategy.GREATER_THAN_OR_EQUAL, 6),
              postRequestedFor(urlEqualTo(STATEMENT_PATH)));
    }
  }

  @Test
  void testCompoundStatements() throws SQLException {
    // Insert for compound test
    String tableName = "compound_test_table";
    setupDatabaseTable(connection, tableName);
    insertTestData(connection, tableName);

    // Update operation as part of compound test
    String updateSQL =
        "UPDATE "
            + getFullyQualifiedTableName(tableName)
            + " SET col2 = 'updatedValue2' WHERE id = 1";
    executeSQL(connection, updateSQL);

    // Verify update operation
    ResultSet rs =
        executeQuery(
            connection,
            "SELECT col2 FROM " + getFullyQualifiedTableName(tableName) + " WHERE id = 1");
    assertTrue(
        rs.next() && "updatedValue2".equals(rs.getString("col2")),
        "Expected 'updatedValue2', got " + rs.getString("col2"));

    // Delete operation as part of compound test
    String deleteSQL = "DELETE FROM " + getFullyQualifiedTableName(tableName) + " WHERE id = 1";
    executeSQL(connection, deleteSQL);

    // Verify delete operation
    rs = executeQuery(connection, "SELECT * FROM " + getFullyQualifiedTableName(tableName));
    assertFalse(rs.next(), "Expected no rows after delete");
    deleteTable(connection, tableName);

    if (isSqlExecSdkClient()) {
      // At least 8 statement requests are sent:
      // drop, create, insert, update, select, delete, select, drop
      // There can be more for retries
      getDatabricksApiExtension()
          .verify(
              new CountMatchingStrategy(CountMatchingStrategy.GREATER_THAN_OR_EQUAL, 8),
              postRequestedFor(urlEqualTo(STATEMENT_PATH)));
    }
  }

  @Test
  void testComplexQueryJoins() throws SQLException {
    String table1Name = "table1_cqj";
    String table2Name = "table2_cqj";
    setupDatabaseTable(connection, table1Name);
    setupDatabaseTable(connection, table2Name);
    insertTestDataForJoins(connection, table1Name, table2Name);

    String joinSQL =
        "SELECT t1.id, t2.col2 FROM "
            + getFullyQualifiedTableName(table1Name)
            + " t1 "
            + "JOIN "
            + getFullyQualifiedTableName(table2Name)
            + " t2 "
            + "ON t1.id = t2.id";
    ResultSet rs = executeQuery(connection, joinSQL);
    assertTrue(rs.next(), "Expected at least one row from JOIN query");
    deleteTable(connection, table1Name);
    deleteTable(connection, table2Name);

    if (isSqlExecSdkClient()) {
      // At least 11 statement requests are sent:
      // drop table1, create table1, drop table2, create table2, insert table1, insert table1,
      // insert table2, insert table2, select join, drop table1, drop table2
      getDatabricksApiExtension()
          .verify(
              new CountMatchingStrategy(CountMatchingStrategy.GREATER_THAN_OR_EQUAL, 11),
              postRequestedFor(urlEqualTo(STATEMENT_PATH)));
    }
  }

  @Test
  void testComplexQuerySubqueries() throws SQLException {
    String tableName = "subquery_test_table";
    setupDatabaseTable(connection, tableName);
    insertTestData(connection, tableName);

    String subquerySQL =
        "SELECT id FROM "
            + getFullyQualifiedTableName(tableName)
            + " WHERE id IN (SELECT id FROM "
            + getFullyQualifiedTableName(tableName)
            + " WHERE col1 = 'value1')";
    ResultSet rs = executeQuery(connection, subquerySQL);
    assertTrue(rs.next(), "Expected at least one row from subquery");
    deleteTable(connection, tableName);

    if (isSqlExecSdkClient()) {
      // At least 5 statement requests are sent: drop, create, insert, select, drop
      // There can be more for retries
      getDatabricksApiExtension()
          .verify(
              new CountMatchingStrategy(CountMatchingStrategy.GREATER_THAN_OR_EQUAL, 5),
              postRequestedFor(urlEqualTo(STATEMENT_PATH)));
    }
  }

  @Test
  void testExecuteAsyncStatement() throws Exception {
    Statement s = connection.createStatement();
    IDatabricksStatement ids = s.unwrap(IDatabricksStatement.class);

    // Takes approx 10s to complete
    String sql =
        "WITH a AS (\n"
            + "    SELECT id AS x FROM range(1, 1000000)\n"
            + "),\n"
            + "b AS (\n"
            + "    SELECT id AS y FROM range(1, 1000000)\n"
            + ")\n"
            + "SELECT a.x, b.y\n"
            + "FROM a\n"
            + "JOIN b\n"
            + "  ON (a.x * b.y) % 1234567 = 1\n"
            + "WHERE a.x < 100\n"
            + "LIMIT 10;";

    // Execute asynchronously
    ResultSet rs = ids.executeAsync(sql);
    StatementState state = rs.unwrap(IDatabricksResultSet.class).getStatementStatus().getState();

    // Poll for status
    while (state != StatementState.SUCCEEDED && state != StatementState.FAILED) {
      Thread.sleep(1000);
      rs = s.unwrap(IDatabricksStatement.class).getExecutionResult();
      state = rs.unwrap(IDatabricksResultSet.class).getStatementStatus().getState();
    }

    // Second connection
    Connection con2 = getValidJDBCConnection();
    IDatabricksConnection idc = con2.unwrap(IDatabricksConnection.class);
    Statement stm = idc.getStatement(rs.unwrap(IDatabricksResultSet.class).getStatementId());
    ResultSet rs2 = stm.unwrap(IDatabricksStatement.class).getExecutionResult();
    assertEquals(
        StatementState.SUCCEEDED,
        rs2.unwrap(IDatabricksResultSet.class).getStatementStatus().getState());
  }
}
