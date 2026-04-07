package com.databricks.jdbc.api.impl;

import com.databricks.jdbc.exception.DatabricksSQLFeatureNotSupportedException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.Calendar;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Databricks implementation of {@link CallableStatement} with IN-parameter-only support.
 *
 * <p>This class extends {@link DatabricksPreparedStatement} to reuse all IN parameter binding
 * ({@code setXXX(int, value)}), execution, and batch functionality. The JDBC escape syntax {@code
 * {call proc(?, ?)}} is converted to {@code CALL proc(?, ?)} by the existing escape processing in
 * {@link com.databricks.jdbc.common.util.StringUtil#convertJdbcEscapeSequences}.
 *
 * <p>OUT and INOUT parameters are not supported. All {@code registerOutParameter()}, output
 * retrieval ({@code getXXX(int/String)}), and named parameter ({@code setXXX(String, value)})
 * methods throw {@link SQLFeatureNotSupportedException}.
 */
public class DatabricksCallableStatement extends DatabricksPreparedStatement
    implements CallableStatement {

  private static final JdbcLogger LOGGER =
      JdbcLoggerFactory.getLogger(DatabricksCallableStatement.class);

  /**
   * Pattern to detect the JDBC return-value escape syntax: {@code {? = call ...}}. This syntax
   * requires server-side support for typed return values which is not available.
   */
  private static final Pattern RETURN_VALUE_SYNTAX =
      Pattern.compile("\\{\\s*\\?\\s*=\\s*call\\b", Pattern.CASE_INSENSITIVE);

  /** Matches JDBC callable escape syntax: {@code {call proc(...)}}. */
  private static final Pattern CALL_ESCAPE_SYNTAX =
      Pattern.compile("\\{\\s*call\\s+([^}]*)\\}", Pattern.CASE_INSENSITIVE);

  private static final String OUT_PARAM_NOT_SUPPORTED =
      "OUT and INOUT parameters are not supported. "
          + "Only IN parameters are supported for callable statements. "
          + "Use SQL scripting with DECLARE and SELECT as a workaround for output values.";

  private static final String NAMED_PARAM_NOT_SUPPORTED =
      "Named parameters are not supported. Use positional parameters (setXXX(int, value)) instead.";

  public DatabricksCallableStatement(DatabricksConnection connection, String sql)
      throws SQLException {
    // Convert {call proc(?)} to CALL proc(?) before passing to parent so that:
    // 1. The parent's shouldReturnResultSet matches CALL_PATTERN correctly
    // 2. The conversion is independent of the escapeProcessing flag
    // 3. Other JDBC escape sequences ({d ...}, {fn ...}, etc.) remain controlled
    //    by setEscapeProcessing as usual
    super(connection, convertCallEscapeSyntax(sql));
    validateNoReturnValueSyntax(sql);
    LOGGER.debug("Created DatabricksCallableStatement for SQL: {}", sql);
  }

  /**
   * Converts JDBC callable escape syntax {@code {call proc(?, ?)}} to native {@code CALL proc(?,
   * ?)}. This is done unconditionally (not gated by escapeProcessing) because the {@code {call
   * ...}} form is specific to callable statements and must always be resolved. Other JDBC escape
   * sequences are handled independently by the standard escape processing path.
   */
  private static String convertCallEscapeSyntax(String sql) {
    if (sql == null) {
      return null;
    }
    return CALL_ESCAPE_SYNTAX.matcher(sql).replaceAll("CALL $1");
  }

  private void validateNoReturnValueSyntax(String sql) throws SQLException {
    if (sql != null && RETURN_VALUE_SYNTAX.matcher(sql).find()) {
      throw new DatabricksSQLFeatureNotSupportedException(
          "Return value syntax {? = call ...} is not supported. "
              + "Use {call proc(...)} and retrieve results via the ResultSet.");
    }
  }

  /**
   * Throws for methods with non-void return types. Always throws, never returns. The generic return
   * type avoids separate helpers for each return type.
   */
  private <T> T throwOutParamNotSupported() throws SQLException {
    throw new DatabricksSQLFeatureNotSupportedException(OUT_PARAM_NOT_SUPPORTED);
  }

  /** Throws for void methods (registerOutParameter, named setXXX). */
  private void throwOutParamNotSupportedVoid() throws SQLException {
    throw new DatabricksSQLFeatureNotSupportedException(OUT_PARAM_NOT_SUPPORTED);
  }

  private void throwNamedParamNotSupportedVoid() throws SQLException {
    throw new DatabricksSQLFeatureNotSupportedException(NAMED_PARAM_NOT_SUPPORTED);
  }

  // ---------------------------------------------------------------------------
  // registerOutParameter — all overloads
  // ---------------------------------------------------------------------------

  @Override
  public void registerOutParameter(int parameterIndex, int sqlType) throws SQLException {
    throwOutParamNotSupportedVoid();
  }

  @Override
  public void registerOutParameter(int parameterIndex, int sqlType, int scale) throws SQLException {
    throwOutParamNotSupportedVoid();
  }

  @Override
  public void registerOutParameter(int parameterIndex, int sqlType, String typeName)
      throws SQLException {
    throwOutParamNotSupportedVoid();
  }

  @Override
  public void registerOutParameter(String parameterName, int sqlType) throws SQLException {
    throwOutParamNotSupportedVoid();
  }

  @Override
  public void registerOutParameter(String parameterName, int sqlType, int scale)
      throws SQLException {
    throwOutParamNotSupportedVoid();
  }

  @Override
  public void registerOutParameter(String parameterName, int sqlType, String typeName)
      throws SQLException {
    throwOutParamNotSupportedVoid();
  }

  // Java 8+ SQLType-based overrides — explicit overrides to ensure consistent
  // Databricks-specific error messages rather than relying on default method delegation.

  @Override
  public void registerOutParameter(int parameterIndex, SQLType sqlType) throws SQLException {
    throwOutParamNotSupportedVoid();
  }

  @Override
  public void registerOutParameter(int parameterIndex, SQLType sqlType, int scale)
      throws SQLException {
    throwOutParamNotSupportedVoid();
  }

  @Override
  public void registerOutParameter(int parameterIndex, SQLType sqlType, String typeName)
      throws SQLException {
    throwOutParamNotSupportedVoid();
  }

  @Override
  public void registerOutParameter(String parameterName, SQLType sqlType) throws SQLException {
    throwOutParamNotSupportedVoid();
  }

  @Override
  public void registerOutParameter(String parameterName, SQLType sqlType, int scale)
      throws SQLException {
    throwOutParamNotSupportedVoid();
  }

  @Override
  public void registerOutParameter(String parameterName, SQLType sqlType, String typeName)
      throws SQLException {
    throwOutParamNotSupportedVoid();
  }

  // ---------------------------------------------------------------------------
  // wasNull
  // ---------------------------------------------------------------------------

  @Override
  public boolean wasNull() throws SQLException {
    return throwOutParamNotSupported();
  }

  // ---------------------------------------------------------------------------
  // Output parameter retrieval by index — getXXX(int)
  // ---------------------------------------------------------------------------

  @Override
  public String getString(int parameterIndex) throws SQLException {
    return throwOutParamNotSupported();
  }

  @Override
  public boolean getBoolean(int parameterIndex) throws SQLException {
    return throwOutParamNotSupported();
  }

  @Override
  public byte getByte(int parameterIndex) throws SQLException {
    return throwOutParamNotSupported();
  }

  @Override
  public short getShort(int parameterIndex) throws SQLException {
    return throwOutParamNotSupported();
  }

  @Override
  public int getInt(int parameterIndex) throws SQLException {
    return throwOutParamNotSupported();
  }

  @Override
  public long getLong(int parameterIndex) throws SQLException {
    return throwOutParamNotSupported();
  }

  @Override
  public float getFloat(int parameterIndex) throws SQLException {
    return throwOutParamNotSupported();
  }

  @Override
  public double getDouble(int parameterIndex) throws SQLException {
    return throwOutParamNotSupported();
  }

  @Override
  @SuppressWarnings("deprecation")
  public BigDecimal getBigDecimal(int parameterIndex, int scale) throws SQLException {
    return throwOutParamNotSupported();
  }

  @Override
  public byte[] getBytes(int parameterIndex) throws SQLException {
    return throwOutParamNotSupported();
  }

  @Override
  public Date getDate(int parameterIndex) throws SQLException {
    return throwOutParamNotSupported();
  }

  @Override
  public Time getTime(int parameterIndex) throws SQLException {
    return throwOutParamNotSupported();
  }

  @Override
  public Timestamp getTimestamp(int parameterIndex) throws SQLException {
    return throwOutParamNotSupported();
  }

  @Override
  public Object getObject(int parameterIndex) throws SQLException {
    return throwOutParamNotSupported();
  }

  @Override
  public BigDecimal getBigDecimal(int parameterIndex) throws SQLException {
    return throwOutParamNotSupported();
  }

  @Override
  public Object getObject(int parameterIndex, Map<String, Class<?>> map) throws SQLException {
    return throwOutParamNotSupported();
  }

  @Override
  public Ref getRef(int parameterIndex) throws SQLException {
    return throwOutParamNotSupported();
  }

  @Override
  public Blob getBlob(int parameterIndex) throws SQLException {
    return throwOutParamNotSupported();
  }

  @Override
  public Clob getClob(int parameterIndex) throws SQLException {
    return throwOutParamNotSupported();
  }

  @Override
  public Array getArray(int parameterIndex) throws SQLException {
    return throwOutParamNotSupported();
  }

  @Override
  public Date getDate(int parameterIndex, Calendar cal) throws SQLException {
    return throwOutParamNotSupported();
  }

  @Override
  public Time getTime(int parameterIndex, Calendar cal) throws SQLException {
    return throwOutParamNotSupported();
  }

  @Override
  public Timestamp getTimestamp(int parameterIndex, Calendar cal) throws SQLException {
    return throwOutParamNotSupported();
  }

  @Override
  public URL getURL(int parameterIndex) throws SQLException {
    return throwOutParamNotSupported();
  }

  @Override
  public RowId getRowId(int parameterIndex) throws SQLException {
    return throwOutParamNotSupported();
  }

  @Override
  public NClob getNClob(int parameterIndex) throws SQLException {
    return throwOutParamNotSupported();
  }

  @Override
  public SQLXML getSQLXML(int parameterIndex) throws SQLException {
    return throwOutParamNotSupported();
  }

  @Override
  public String getNString(int parameterIndex) throws SQLException {
    return throwOutParamNotSupported();
  }

  @Override
  public Reader getNCharacterStream(int parameterIndex) throws SQLException {
    return throwOutParamNotSupported();
  }

  @Override
  public Reader getCharacterStream(int parameterIndex) throws SQLException {
    return throwOutParamNotSupported();
  }

  @Override
  public <T> T getObject(int parameterIndex, Class<T> type) throws SQLException {
    return throwOutParamNotSupported();
  }

  // ---------------------------------------------------------------------------
  // Output parameter retrieval by name — getXXX(String)
  // ---------------------------------------------------------------------------

  @Override
  public String getString(String parameterName) throws SQLException {
    return throwOutParamNotSupported();
  }

  @Override
  public boolean getBoolean(String parameterName) throws SQLException {
    return throwOutParamNotSupported();
  }

  @Override
  public byte getByte(String parameterName) throws SQLException {
    return throwOutParamNotSupported();
  }

  @Override
  public short getShort(String parameterName) throws SQLException {
    return throwOutParamNotSupported();
  }

  @Override
  public int getInt(String parameterName) throws SQLException {
    return throwOutParamNotSupported();
  }

  @Override
  public long getLong(String parameterName) throws SQLException {
    return throwOutParamNotSupported();
  }

  @Override
  public float getFloat(String parameterName) throws SQLException {
    return throwOutParamNotSupported();
  }

  @Override
  public double getDouble(String parameterName) throws SQLException {
    return throwOutParamNotSupported();
  }

  @Override
  public byte[] getBytes(String parameterName) throws SQLException {
    return throwOutParamNotSupported();
  }

  @Override
  public Date getDate(String parameterName) throws SQLException {
    return throwOutParamNotSupported();
  }

  @Override
  public Time getTime(String parameterName) throws SQLException {
    return throwOutParamNotSupported();
  }

  @Override
  public Timestamp getTimestamp(String parameterName) throws SQLException {
    return throwOutParamNotSupported();
  }

  @Override
  public Object getObject(String parameterName) throws SQLException {
    return throwOutParamNotSupported();
  }

  @Override
  public BigDecimal getBigDecimal(String parameterName) throws SQLException {
    return throwOutParamNotSupported();
  }

  @Override
  public Object getObject(String parameterName, Map<String, Class<?>> map) throws SQLException {
    return throwOutParamNotSupported();
  }

  @Override
  public Ref getRef(String parameterName) throws SQLException {
    return throwOutParamNotSupported();
  }

  @Override
  public Blob getBlob(String parameterName) throws SQLException {
    return throwOutParamNotSupported();
  }

  @Override
  public Clob getClob(String parameterName) throws SQLException {
    return throwOutParamNotSupported();
  }

  @Override
  public Array getArray(String parameterName) throws SQLException {
    return throwOutParamNotSupported();
  }

  @Override
  public Date getDate(String parameterName, Calendar cal) throws SQLException {
    return throwOutParamNotSupported();
  }

  @Override
  public Time getTime(String parameterName, Calendar cal) throws SQLException {
    return throwOutParamNotSupported();
  }

  @Override
  public Timestamp getTimestamp(String parameterName, Calendar cal) throws SQLException {
    return throwOutParamNotSupported();
  }

  @Override
  public URL getURL(String parameterName) throws SQLException {
    return throwOutParamNotSupported();
  }

  @Override
  public RowId getRowId(String parameterName) throws SQLException {
    return throwOutParamNotSupported();
  }

  @Override
  public NClob getNClob(String parameterName) throws SQLException {
    return throwOutParamNotSupported();
  }

  @Override
  public SQLXML getSQLXML(String parameterName) throws SQLException {
    return throwOutParamNotSupported();
  }

  @Override
  public String getNString(String parameterName) throws SQLException {
    return throwOutParamNotSupported();
  }

  @Override
  public Reader getNCharacterStream(String parameterName) throws SQLException {
    return throwOutParamNotSupported();
  }

  @Override
  public Reader getCharacterStream(String parameterName) throws SQLException {
    return throwOutParamNotSupported();
  }

  @Override
  public <T> T getObject(String parameterName, Class<T> type) throws SQLException {
    return throwOutParamNotSupported();
  }

  // ---------------------------------------------------------------------------
  // Named parameter setXXX(String, ...) methods
  // ---------------------------------------------------------------------------

  @Override
  public void setURL(String parameterName, URL val) throws SQLException {
    throwNamedParamNotSupportedVoid();
  }

  @Override
  public void setNull(String parameterName, int sqlType) throws SQLException {
    throwNamedParamNotSupportedVoid();
  }

  @Override
  public void setBoolean(String parameterName, boolean x) throws SQLException {
    throwNamedParamNotSupportedVoid();
  }

  @Override
  public void setByte(String parameterName, byte x) throws SQLException {
    throwNamedParamNotSupportedVoid();
  }

  @Override
  public void setShort(String parameterName, short x) throws SQLException {
    throwNamedParamNotSupportedVoid();
  }

  @Override
  public void setInt(String parameterName, int x) throws SQLException {
    throwNamedParamNotSupportedVoid();
  }

  @Override
  public void setLong(String parameterName, long x) throws SQLException {
    throwNamedParamNotSupportedVoid();
  }

  @Override
  public void setFloat(String parameterName, float x) throws SQLException {
    throwNamedParamNotSupportedVoid();
  }

  @Override
  public void setDouble(String parameterName, double x) throws SQLException {
    throwNamedParamNotSupportedVoid();
  }

  @Override
  public void setBigDecimal(String parameterName, BigDecimal x) throws SQLException {
    throwNamedParamNotSupportedVoid();
  }

  @Override
  public void setString(String parameterName, String x) throws SQLException {
    throwNamedParamNotSupportedVoid();
  }

  @Override
  public void setBytes(String parameterName, byte[] x) throws SQLException {
    throwNamedParamNotSupportedVoid();
  }

  @Override
  public void setDate(String parameterName, Date x) throws SQLException {
    throwNamedParamNotSupportedVoid();
  }

  @Override
  public void setTime(String parameterName, Time x) throws SQLException {
    throwNamedParamNotSupportedVoid();
  }

  @Override
  public void setTimestamp(String parameterName, Timestamp x) throws SQLException {
    throwNamedParamNotSupportedVoid();
  }

  @Override
  public void setAsciiStream(String parameterName, InputStream x, int length) throws SQLException {
    throwNamedParamNotSupportedVoid();
  }

  @Override
  public void setBinaryStream(String parameterName, InputStream x, int length) throws SQLException {
    throwNamedParamNotSupportedVoid();
  }

  @Override
  public void setObject(String parameterName, Object x, int targetSqlType, int scale)
      throws SQLException {
    throwNamedParamNotSupportedVoid();
  }

  @Override
  public void setObject(String parameterName, Object x, int targetSqlType) throws SQLException {
    throwNamedParamNotSupportedVoid();
  }

  @Override
  public void setObject(String parameterName, Object x) throws SQLException {
    throwNamedParamNotSupportedVoid();
  }

  @Override
  public void setCharacterStream(String parameterName, Reader reader, int length)
      throws SQLException {
    throwNamedParamNotSupportedVoid();
  }

  @Override
  public void setDate(String parameterName, Date x, Calendar cal) throws SQLException {
    throwNamedParamNotSupportedVoid();
  }

  @Override
  public void setTime(String parameterName, Time x, Calendar cal) throws SQLException {
    throwNamedParamNotSupportedVoid();
  }

  @Override
  public void setTimestamp(String parameterName, Timestamp x, Calendar cal) throws SQLException {
    throwNamedParamNotSupportedVoid();
  }

  @Override
  public void setNull(String parameterName, int sqlType, String typeName) throws SQLException {
    throwNamedParamNotSupportedVoid();
  }

  @Override
  public void setRowId(String parameterName, RowId x) throws SQLException {
    throwNamedParamNotSupportedVoid();
  }

  @Override
  public void setNString(String parameterName, String value) throws SQLException {
    throwNamedParamNotSupportedVoid();
  }

  @Override
  public void setNCharacterStream(String parameterName, Reader value, long length)
      throws SQLException {
    throwNamedParamNotSupportedVoid();
  }

  @Override
  public void setNClob(String parameterName, NClob value) throws SQLException {
    throwNamedParamNotSupportedVoid();
  }

  @Override
  public void setClob(String parameterName, Reader reader, long length) throws SQLException {
    throwNamedParamNotSupportedVoid();
  }

  @Override
  public void setBlob(String parameterName, InputStream inputStream, long length)
      throws SQLException {
    throwNamedParamNotSupportedVoid();
  }

  @Override
  public void setNClob(String parameterName, Reader reader, long length) throws SQLException {
    throwNamedParamNotSupportedVoid();
  }

  @Override
  public void setSQLXML(String parameterName, SQLXML xmlObject) throws SQLException {
    throwNamedParamNotSupportedVoid();
  }

  @Override
  public void setBlob(String parameterName, Blob x) throws SQLException {
    throwNamedParamNotSupportedVoid();
  }

  @Override
  public void setClob(String parameterName, Clob x) throws SQLException {
    throwNamedParamNotSupportedVoid();
  }

  @Override
  public void setAsciiStream(String parameterName, InputStream x, long length) throws SQLException {
    throwNamedParamNotSupportedVoid();
  }

  @Override
  public void setBinaryStream(String parameterName, InputStream x, long length)
      throws SQLException {
    throwNamedParamNotSupportedVoid();
  }

  @Override
  public void setCharacterStream(String parameterName, Reader reader, long length)
      throws SQLException {
    throwNamedParamNotSupportedVoid();
  }

  @Override
  public void setAsciiStream(String parameterName, InputStream x) throws SQLException {
    throwNamedParamNotSupportedVoid();
  }

  @Override
  public void setBinaryStream(String parameterName, InputStream x) throws SQLException {
    throwNamedParamNotSupportedVoid();
  }

  @Override
  public void setCharacterStream(String parameterName, Reader reader) throws SQLException {
    throwNamedParamNotSupportedVoid();
  }

  @Override
  public void setNCharacterStream(String parameterName, Reader value) throws SQLException {
    throwNamedParamNotSupportedVoid();
  }

  @Override
  public void setClob(String parameterName, Reader reader) throws SQLException {
    throwNamedParamNotSupportedVoid();
  }

  @Override
  public void setBlob(String parameterName, InputStream inputStream) throws SQLException {
    throwNamedParamNotSupportedVoid();
  }

  @Override
  public void setNClob(String parameterName, Reader reader) throws SQLException {
    throwNamedParamNotSupportedVoid();
  }

  // Java 8+ SQLType-based named parameter overrides

  @Override
  public void setObject(String parameterName, Object x, SQLType targetSqlType, int scaleOrLength)
      throws SQLException {
    throwNamedParamNotSupportedVoid();
  }

  @Override
  public void setObject(String parameterName, Object x, SQLType targetSqlType) throws SQLException {
    throwNamedParamNotSupportedVoid();
  }
}
