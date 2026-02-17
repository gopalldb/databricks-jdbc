package com.databricks.jdbc.integration.fakeservice.tests;

import static com.databricks.jdbc.integration.IntegrationTestUtil.*;
import static org.junit.jupiter.api.Assertions.*;

import com.databricks.jdbc.api.impl.DatabricksConnection;
import com.databricks.jdbc.common.DatabricksClientType;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.exception.DatabricksSQLFeatureNotImplementedException;
import com.databricks.jdbc.integration.fakeservice.AbstractFakeServiceIntegrationTests;
import com.databricks.jdbc.integration.fakeservice.FakeServiceExtension;
import java.sql.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for JDBC method mismatch scenarios. Verifies that calling executeQuery() with
 * DML statements and executeUpdate() with SELECT queries produces appropriate errors or behavior
 * per JDBC spec. Also verifies PreparedStatement rejects SQL-accepting Statement methods.
 */
public class MethodMismatchIntegrationTests extends AbstractFakeServiceIntegrationTests {

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
  void testExecuteQuery_WithInsert_ThrowsSQLException() throws SQLException {
    String tableName = "mismatch_insert_table";
    setupDatabaseTable(connection, tableName);

    Statement stmt = connection.createStatement();
    String insertSQL =
        "INSERT INTO "
            + getFullyQualifiedTableName(tableName)
            + " (id, col1, col2) VALUES (1, 'a', 'b')";

    // executeQuery() with INSERT should throw because no ResultSet is generated
    DatabricksSQLException e =
        assertThrows(DatabricksSQLException.class, () -> stmt.executeQuery(insertSQL));
    assertTrue(
        e.getMessage().contains("ResultSet was expected but not generated"),
        "Error message should indicate no ResultSet was generated, got: " + e.getMessage());

    deleteTable(connection, tableName);
  }

  @Test
  void testExecuteQuery_WithUpdate_ThrowsSQLException() throws SQLException {
    String tableName = "mismatch_update_table";
    setupDatabaseTable(connection, tableName);
    insertTestData(connection, tableName);

    Statement stmt = connection.createStatement();
    String updateSQL =
        "UPDATE " + getFullyQualifiedTableName(tableName) + " SET col1 = 'updated' WHERE id = 1";

    // executeQuery() with UPDATE should throw because no ResultSet is generated
    DatabricksSQLException e =
        assertThrows(DatabricksSQLException.class, () -> stmt.executeQuery(updateSQL));
    assertTrue(
        e.getMessage().contains("ResultSet was expected but not generated"),
        "Error message should indicate no ResultSet was generated, got: " + e.getMessage());

    deleteTable(connection, tableName);
  }

  @Test
  void testExecuteQuery_WithDelete_ThrowsSQLException() throws SQLException {
    String tableName = "mismatch_delete_table";
    setupDatabaseTable(connection, tableName);
    insertTestData(connection, tableName);

    Statement stmt = connection.createStatement();
    String deleteSQL = "DELETE FROM " + getFullyQualifiedTableName(tableName) + " WHERE id = 1";

    // executeQuery() with DELETE should throw because no ResultSet is generated
    DatabricksSQLException e =
        assertThrows(DatabricksSQLException.class, () -> stmt.executeQuery(deleteSQL));
    assertTrue(
        e.getMessage().contains("ResultSet was expected but not generated"),
        "Error message should indicate no ResultSet was generated, got: " + e.getMessage());

    deleteTable(connection, tableName);
  }

  @Test
  void testExecuteUpdate_WithSelect_ThrowsSQLException() throws SQLException {
    // executeUpdate with SELECT executes the query with StatementType.UPDATE,
    // then tries to get update count from the result. Since SELECT results don't have
    // the num_affected_rows column, this throws DatabricksSQLException.
    Statement stmt = connection.createStatement();

    assertThrows(
        DatabricksSQLException.class,
        () -> stmt.executeUpdate("SELECT 1 AS num"),
        "executeUpdate() with SELECT should throw because result has no update count column");
  }

  @Test
  void testPreparedStatement_StatementMethodsWithSQL_ThrowException() throws SQLException {
    String selectSQL = "SELECT 1";
    PreparedStatement pstmt = connection.prepareStatement(selectSQL);

    // Per JDBC spec, PreparedStatement should reject SQL-accepting Statement methods
    assertThrows(
        DatabricksSQLFeatureNotImplementedException.class,
        () -> pstmt.executeQuery("SELECT 2"),
        "PreparedStatement.executeQuery(String) should throw");

    assertThrows(
        DatabricksSQLFeatureNotImplementedException.class,
        () -> pstmt.executeUpdate("INSERT INTO dummy VALUES (1)"),
        "PreparedStatement.executeUpdate(String) should throw");

    assertThrows(
        DatabricksSQLFeatureNotImplementedException.class,
        () -> pstmt.execute("SELECT 3"),
        "PreparedStatement.execute(String) should throw");
  }
}
