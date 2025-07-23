package com.databricks.jdbc.exception;

import static com.databricks.jdbc.telemetry.TelemetryHelper.exportFailureLog;

import com.databricks.jdbc.common.util.DatabricksThreadContextHolder;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import java.sql.SQLException;

/** Top level exception for Databricks driver */
public class DatabricksSQLException extends SQLException {
  public DatabricksSQLException(String reason, DatabricksDriverErrorCode internalError) {
    this(reason, internalError.name());
  }

  public DatabricksSQLException(
      String reason, DatabricksDriverErrorCode internalError, boolean silentExceptions) {
    this(reason, internalError.name(), silentExceptions);
  }

  public DatabricksSQLException(
      String reason, Throwable cause, DatabricksDriverErrorCode internalError) {
    this(reason, cause, internalError.toString());
  }

  public DatabricksSQLException(String reason, Throwable cause, String sqlState) {
    super(reason, sqlState, cause);
    exportFailureLog(
        DatabricksThreadContextHolder.getConnectionContext(),
        DatabricksDriverErrorCode.CONNECTION_ERROR.name(),
        reason);
  }

  // This constructor is used to export chunk download failure logs
  public DatabricksSQLException(
      String reason, Throwable cause, String statementId, Long chunkIndex, String sqlState) {
    super(reason, sqlState, cause);
    exportFailureLog(
        DatabricksThreadContextHolder.getConnectionContext(),
        DatabricksDriverErrorCode.CONNECTION_ERROR.name(),
        reason,
        statementId,
        chunkIndex);
  }

  public DatabricksSQLException(String reason, String sqlState) {
    super(reason, sqlState);
    exportFailureLog(DatabricksThreadContextHolder.getConnectionContext(), sqlState, reason);
  }

  public DatabricksSQLException(String reason, String sqlState, boolean silentExceptions) {
    super(reason, sqlState);
    if (!silentExceptions) {
      exportFailureLog(DatabricksThreadContextHolder.getConnectionContext(), sqlState, reason);
    }
  }

  public DatabricksSQLException(
      String reason, String sqlState, DatabricksDriverErrorCode internalError) {
    super(reason, sqlState);
    exportFailureLog(
        DatabricksThreadContextHolder.getConnectionContext(), internalError.name(), reason);
  }
}
