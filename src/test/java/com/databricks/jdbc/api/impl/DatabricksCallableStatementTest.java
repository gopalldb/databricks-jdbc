package com.databricks.jdbc.api.impl;

import static com.databricks.jdbc.TestConstants.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.api.internal.IDatabricksSession;
import com.databricks.jdbc.common.StatementType;
import com.databricks.jdbc.common.Warehouse;
import com.databricks.jdbc.dbclient.impl.sqlexec.DatabricksSdkClient;
import com.databricks.jdbc.exception.DatabricksSQLFeatureNotSupportedException;
import java.math.BigDecimal;
import java.sql.*;
import java.util.HashMap;
import java.util.Properties;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class DatabricksCallableStatementTest {

  private static final String WAREHOUSE_ID = "99999999";
  private static final String JDBC_URL =
      "jdbc:databricks://sample-host.18.azuredatabricks.net:4423/default;"
          + "transportMode=http;ssl=1;AuthMech=3;"
          + "httpPath=/sql/1.0/warehouses/99999999;";
  private static final String CALL_SQL = "{call my_proc(?, ?)}";
  // Note: DEFAULT_ESCAPE_PROCESSING is false, so the raw SQL is passed to executeStatement
  private static final String CALL_SQL_AS_EXECUTED = CALL_SQL;

  @Mock DatabricksResultSet resultSet;
  @Mock DatabricksSdkClient client;

  @Nested
  @DisplayName("Construction tests")
  class ConstructionTests {

    @Test
    @DisplayName("prepareCall creates a DatabricksCallableStatement")
    void testPrepareCallCreatesCallableStatement() throws Exception {
      IDatabricksConnectionContext ctx =
          DatabricksConnectionContext.parse(JDBC_URL, new Properties());
      DatabricksConnection connection = new DatabricksConnection(ctx, client);

      CallableStatement stmt = connection.prepareCall(CALL_SQL);
      assertNotNull(stmt);
      assertInstanceOf(DatabricksCallableStatement.class, stmt);
      assertInstanceOf(PreparedStatement.class, stmt);
      assertInstanceOf(CallableStatement.class, stmt);
      stmt.close();
    }

    @Test
    @DisplayName("prepareCall with CALL syntax (no JDBC escape) works")
    void testPrepareCallWithNativeSyntax() throws Exception {
      IDatabricksConnectionContext ctx =
          DatabricksConnectionContext.parse(JDBC_URL, new Properties());
      DatabricksConnection connection = new DatabricksConnection(ctx, client);

      CallableStatement stmt = connection.prepareCall("CALL my_proc(?, ?)");
      assertNotNull(stmt);
      assertInstanceOf(DatabricksCallableStatement.class, stmt);
      stmt.close();
    }

    @Test
    @DisplayName("{? = call ...} return value syntax is rejected")
    void testReturnValueSyntaxThrows() throws Exception {
      IDatabricksConnectionContext ctx =
          DatabricksConnectionContext.parse(JDBC_URL, new Properties());
      DatabricksConnection connection = new DatabricksConnection(ctx, client);

      assertThrows(
          DatabricksSQLFeatureNotSupportedException.class,
          () -> connection.prepareCall("{? = call my_func()}"));
    }

    @Test
    @DisplayName("{? = call ...} syntax variations are all rejected")
    void testReturnValueSyntaxVariations() throws Exception {
      IDatabricksConnectionContext ctx =
          DatabricksConnectionContext.parse(JDBC_URL, new Properties());
      DatabricksConnection connection = new DatabricksConnection(ctx, client);

      // Various whitespace patterns
      assertThrows(
          DatabricksSQLFeatureNotSupportedException.class,
          () -> connection.prepareCall("{?= call my_func()}"));
      assertThrows(
          DatabricksSQLFeatureNotSupportedException.class,
          () -> connection.prepareCall("{ ? = call my_func()}"));
      assertThrows(
          DatabricksSQLFeatureNotSupportedException.class,
          () -> connection.prepareCall("{  ?  =  call my_func(?, ?)}"));
    }

    @Test
    @DisplayName("prepareCall with unsupported resultSetType throws")
    void testPrepareCallWithUnsupportedResultSetType() throws Exception {
      IDatabricksConnectionContext ctx =
          DatabricksConnectionContext.parse(JDBC_URL, new Properties());
      DatabricksConnection connection = new DatabricksConnection(ctx, client);

      assertThrows(
          DatabricksSQLFeatureNotSupportedException.class,
          () ->
              connection.prepareCall(
                  CALL_SQL, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY));
    }

    @Test
    @DisplayName("prepareCall with valid resultSetType/concurrency succeeds")
    void testPrepareCallWithValidResultSetTypeSucceeds() throws Exception {
      IDatabricksConnectionContext ctx =
          DatabricksConnectionContext.parse(JDBC_URL, new Properties());
      DatabricksConnection connection = new DatabricksConnection(ctx, client);

      CallableStatement stmt =
          connection.prepareCall(CALL_SQL, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
      assertNotNull(stmt);
      assertInstanceOf(DatabricksCallableStatement.class, stmt);
      stmt.close();
    }
  }

  @Nested
  @DisplayName("IN parameter and execution tests")
  class ExecutionTests {

    @Test
    @DisplayName("IN parameters can be bound and execute works")
    void testExecuteWithInParams() throws Exception {
      IDatabricksConnectionContext ctx =
          DatabricksConnectionContext.parse(JDBC_URL, new Properties());
      DatabricksConnection connection = new DatabricksConnection(ctx, client);
      DatabricksCallableStatement stmt = new DatabricksCallableStatement(connection, CALL_SQL);

      stmt.setInt(1, 42);
      stmt.setString(2, "test");

      when(client.executeStatement(
              eq(CALL_SQL_AS_EXECUTED),
              eq(new Warehouse(WAREHOUSE_ID)),
              any(HashMap.class),
              eq(StatementType.SQL),
              any(IDatabricksSession.class),
              eq(stmt),
              any()))
          .thenReturn(resultSet);

      // CALL statements use execute() — not executeQuery() since they are non-query
      boolean hasResultSet = stmt.execute();
      assertFalse(hasResultSet);
      stmt.close();
    }

    @Test
    @DisplayName("executeUpdate works for callable statement")
    void testExecuteUpdate() throws Exception {
      IDatabricksConnectionContext ctx =
          DatabricksConnectionContext.parse(JDBC_URL, new Properties());
      DatabricksConnection connection = new DatabricksConnection(ctx, client);
      DatabricksCallableStatement stmt = new DatabricksCallableStatement(connection, CALL_SQL);

      stmt.setInt(1, 42);
      stmt.setString(2, "test");

      when(client.executeStatement(
              eq(CALL_SQL_AS_EXECUTED),
              eq(new Warehouse(WAREHOUSE_ID)),
              any(HashMap.class),
              eq(StatementType.UPDATE),
              any(IDatabricksSession.class),
              eq(stmt),
              any()))
          .thenReturn(resultSet);

      int count = stmt.executeUpdate();
      assertEquals(0, count);
      stmt.close();
    }

    @Test
    @DisplayName("clearParameters works")
    void testClearParameters() throws Exception {
      IDatabricksConnectionContext ctx =
          DatabricksConnectionContext.parse(JDBC_URL, new Properties());
      DatabricksConnection connection = new DatabricksConnection(ctx, client);
      DatabricksCallableStatement stmt = new DatabricksCallableStatement(connection, CALL_SQL);

      stmt.setInt(1, 42);
      stmt.setString(2, "test");
      assertDoesNotThrow(stmt::clearParameters);
      stmt.close();
    }
  }

  @Nested
  @DisplayName("OUT parameter rejection tests")
  class OutParameterTests {

    @Test
    @DisplayName("registerOutParameter by index throws")
    void testRegisterOutParameterByIndex() throws Exception {
      IDatabricksConnectionContext ctx =
          DatabricksConnectionContext.parse(JDBC_URL, new Properties());
      DatabricksConnection connection = new DatabricksConnection(ctx, client);
      DatabricksCallableStatement stmt = new DatabricksCallableStatement(connection, CALL_SQL);

      assertThrows(
          DatabricksSQLFeatureNotSupportedException.class,
          () -> stmt.registerOutParameter(1, Types.INTEGER));
      assertThrows(
          DatabricksSQLFeatureNotSupportedException.class,
          () -> stmt.registerOutParameter(1, Types.DECIMAL, 2));
      assertThrows(
          DatabricksSQLFeatureNotSupportedException.class,
          () -> stmt.registerOutParameter(1, Types.STRUCT, "MY_TYPE"));
      stmt.close();
    }

    @Test
    @DisplayName("registerOutParameter by name throws")
    void testRegisterOutParameterByName() throws Exception {
      IDatabricksConnectionContext ctx =
          DatabricksConnectionContext.parse(JDBC_URL, new Properties());
      DatabricksConnection connection = new DatabricksConnection(ctx, client);
      DatabricksCallableStatement stmt = new DatabricksCallableStatement(connection, CALL_SQL);

      assertThrows(
          DatabricksSQLFeatureNotSupportedException.class,
          () -> stmt.registerOutParameter("param1", Types.INTEGER));
      assertThrows(
          DatabricksSQLFeatureNotSupportedException.class,
          () -> stmt.registerOutParameter("param1", Types.DECIMAL, 2));
      assertThrows(
          DatabricksSQLFeatureNotSupportedException.class,
          () -> stmt.registerOutParameter("param1", Types.STRUCT, "MY_TYPE"));
      stmt.close();
    }

    @Test
    @DisplayName("wasNull throws")
    void testWasNull() throws Exception {
      IDatabricksConnectionContext ctx =
          DatabricksConnectionContext.parse(JDBC_URL, new Properties());
      DatabricksConnection connection = new DatabricksConnection(ctx, client);
      DatabricksCallableStatement stmt = new DatabricksCallableStatement(connection, CALL_SQL);

      assertThrows(DatabricksSQLFeatureNotSupportedException.class, stmt::wasNull);
      stmt.close();
    }

    @Test
    @DisplayName("getXXX by index throws for all types")
    void testGetByIndexThrows() throws Exception {
      IDatabricksConnectionContext ctx =
          DatabricksConnectionContext.parse(JDBC_URL, new Properties());
      DatabricksConnection connection = new DatabricksConnection(ctx, client);
      DatabricksCallableStatement stmt = new DatabricksCallableStatement(connection, CALL_SQL);

      assertThrows(DatabricksSQLFeatureNotSupportedException.class, () -> stmt.getString(1));
      assertThrows(DatabricksSQLFeatureNotSupportedException.class, () -> stmt.getBoolean(1));
      assertThrows(DatabricksSQLFeatureNotSupportedException.class, () -> stmt.getInt(1));
      assertThrows(DatabricksSQLFeatureNotSupportedException.class, () -> stmt.getLong(1));
      assertThrows(DatabricksSQLFeatureNotSupportedException.class, () -> stmt.getFloat(1));
      assertThrows(DatabricksSQLFeatureNotSupportedException.class, () -> stmt.getDouble(1));
      assertThrows(DatabricksSQLFeatureNotSupportedException.class, () -> stmt.getBigDecimal(1));
      assertThrows(DatabricksSQLFeatureNotSupportedException.class, () -> stmt.getDate(1));
      assertThrows(DatabricksSQLFeatureNotSupportedException.class, () -> stmt.getTimestamp(1));
      assertThrows(DatabricksSQLFeatureNotSupportedException.class, () -> stmt.getObject(1));
      assertThrows(DatabricksSQLFeatureNotSupportedException.class, () -> stmt.getBytes(1));
      assertThrows(DatabricksSQLFeatureNotSupportedException.class, () -> stmt.getBlob(1));
      assertThrows(DatabricksSQLFeatureNotSupportedException.class, () -> stmt.getClob(1));
      assertThrows(DatabricksSQLFeatureNotSupportedException.class, () -> stmt.getArray(1));
      assertThrows(DatabricksSQLFeatureNotSupportedException.class, () -> stmt.getURL(1));
      assertThrows(
          DatabricksSQLFeatureNotSupportedException.class, () -> stmt.getObject(1, String.class));
      stmt.close();
    }

    @Test
    @DisplayName("getXXX by name throws for all types")
    void testGetByNameThrows() throws Exception {
      IDatabricksConnectionContext ctx =
          DatabricksConnectionContext.parse(JDBC_URL, new Properties());
      DatabricksConnection connection = new DatabricksConnection(ctx, client);
      DatabricksCallableStatement stmt = new DatabricksCallableStatement(connection, CALL_SQL);

      assertThrows(DatabricksSQLFeatureNotSupportedException.class, () -> stmt.getString("p"));
      assertThrows(DatabricksSQLFeatureNotSupportedException.class, () -> stmt.getBoolean("p"));
      assertThrows(DatabricksSQLFeatureNotSupportedException.class, () -> stmt.getInt("p"));
      assertThrows(DatabricksSQLFeatureNotSupportedException.class, () -> stmt.getLong("p"));
      assertThrows(DatabricksSQLFeatureNotSupportedException.class, () -> stmt.getDouble("p"));
      assertThrows(DatabricksSQLFeatureNotSupportedException.class, () -> stmt.getBigDecimal("p"));
      assertThrows(DatabricksSQLFeatureNotSupportedException.class, () -> stmt.getDate("p"));
      assertThrows(DatabricksSQLFeatureNotSupportedException.class, () -> stmt.getTimestamp("p"));
      assertThrows(DatabricksSQLFeatureNotSupportedException.class, () -> stmt.getObject("p"));
      assertThrows(
          DatabricksSQLFeatureNotSupportedException.class, () -> stmt.getObject("p", String.class));
      stmt.close();
    }
  }

  @Nested
  @DisplayName("Named parameter rejection tests")
  class NamedParameterTests {

    @Test
    @DisplayName("setXXX by name throws for all types")
    void testSetByNameThrows() throws Exception {
      IDatabricksConnectionContext ctx =
          DatabricksConnectionContext.parse(JDBC_URL, new Properties());
      DatabricksConnection connection = new DatabricksConnection(ctx, client);
      DatabricksCallableStatement stmt = new DatabricksCallableStatement(connection, CALL_SQL);

      assertThrows(
          DatabricksSQLFeatureNotSupportedException.class, () -> stmt.setNull("p", Types.INTEGER));
      assertThrows(
          DatabricksSQLFeatureNotSupportedException.class, () -> stmt.setBoolean("p", true));
      assertThrows(DatabricksSQLFeatureNotSupportedException.class, () -> stmt.setInt("p", 42));
      assertThrows(DatabricksSQLFeatureNotSupportedException.class, () -> stmt.setLong("p", 42L));
      assertThrows(DatabricksSQLFeatureNotSupportedException.class, () -> stmt.setFloat("p", 1.0f));
      assertThrows(DatabricksSQLFeatureNotSupportedException.class, () -> stmt.setDouble("p", 1.0));
      assertThrows(
          DatabricksSQLFeatureNotSupportedException.class,
          () -> stmt.setBigDecimal("p", BigDecimal.ONE));
      assertThrows(
          DatabricksSQLFeatureNotSupportedException.class, () -> stmt.setString("p", "val"));
      assertThrows(
          DatabricksSQLFeatureNotSupportedException.class, () -> stmt.setDate("p", new Date(0)));
      assertThrows(
          DatabricksSQLFeatureNotSupportedException.class,
          () -> stmt.setTimestamp("p", new Timestamp(0)));
      assertThrows(
          DatabricksSQLFeatureNotSupportedException.class, () -> stmt.setObject("p", "val"));
      assertThrows(
          DatabricksSQLFeatureNotSupportedException.class,
          () -> stmt.setObject("p", "val", Types.VARCHAR));
      stmt.close();
    }
  }

  @Nested
  @DisplayName("Exception message tests")
  class ExceptionMessageTests {

    @Test
    @DisplayName("OUT parameter exception has clear message")
    void testOutParamExceptionMessage() throws Exception {
      IDatabricksConnectionContext ctx =
          DatabricksConnectionContext.parse(JDBC_URL, new Properties());
      DatabricksConnection connection = new DatabricksConnection(ctx, client);
      DatabricksCallableStatement stmt = new DatabricksCallableStatement(connection, CALL_SQL);

      DatabricksSQLFeatureNotSupportedException ex =
          assertThrows(
              DatabricksSQLFeatureNotSupportedException.class,
              () -> stmt.registerOutParameter(1, Types.INTEGER));
      assertTrue(ex.getMessage().contains("OUT and INOUT parameters are not supported"));
      stmt.close();
    }

    @Test
    @DisplayName("Named parameter exception has clear message")
    void testNamedParamExceptionMessage() throws Exception {
      IDatabricksConnectionContext ctx =
          DatabricksConnectionContext.parse(JDBC_URL, new Properties());
      DatabricksConnection connection = new DatabricksConnection(ctx, client);
      DatabricksCallableStatement stmt = new DatabricksCallableStatement(connection, CALL_SQL);

      DatabricksSQLFeatureNotSupportedException ex =
          assertThrows(
              DatabricksSQLFeatureNotSupportedException.class,
              () -> stmt.setString("param1", "value"));
      assertTrue(ex.getMessage().contains("Named parameters are not supported"));
      stmt.close();
    }

    @Test
    @DisplayName("Return value syntax exception has clear message")
    void testReturnValueExceptionMessage() throws Exception {
      IDatabricksConnectionContext ctx =
          DatabricksConnectionContext.parse(JDBC_URL, new Properties());
      DatabricksConnection connection = new DatabricksConnection(ctx, client);

      DatabricksSQLFeatureNotSupportedException ex =
          assertThrows(
              DatabricksSQLFeatureNotSupportedException.class,
              () -> connection.prepareCall("{? = call my_func()}"));
      assertTrue(ex.getMessage().contains("{? = call ...}"));
      assertTrue(ex.getMessage().contains("not supported"));
    }
  }
}
