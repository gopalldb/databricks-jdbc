package com.databricks.jdbc.exception;

import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;

/** Exception class to handle http errors while downloading chunk data from external links. */
public class DatabricksHttpException extends DatabricksSQLException {

  private final int httpStatusCode;

  public DatabricksHttpException(
      String message, Throwable cause, DatabricksDriverErrorCode sqlCode) {
    super(message, cause, sqlCode);
    this.httpStatusCode = 0;
  }

  public DatabricksHttpException(String message, DatabricksDriverErrorCode internalCode) {
    super(message, null, internalCode.toString());
    this.httpStatusCode = 0;
  }

  public DatabricksHttpException(String message, String sqlState) {
    super(message, null, sqlState);
    this.httpStatusCode = 0;
  }

  public DatabricksHttpException(String message, Throwable throwable, String sqlState) {
    super(message, throwable, sqlState);
    this.httpStatusCode = 0;
  }

  public DatabricksHttpException(String message, String sqlState, int httpStatusCode) {
    super(message, null, sqlState);
    this.httpStatusCode = httpStatusCode;
  }

  /** Returns the HTTP status code, or 0 if not set. */
  public int getHttpStatusCode() {
    return httpStatusCode;
  }
}
