package com.databricks.jdbc.api.impl;

import com.databricks.jdbc.api.internal.IDatabricksSession;
import com.databricks.jdbc.dbclient.impl.common.StatementId;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.model.core.ColumnInfo;
import com.databricks.jdbc.model.core.ColumnInfoTypeName;
import com.databricks.jdbc.model.core.ResultData;
import com.databricks.jdbc.model.core.ResultManifest;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import java.util.*;
import java.util.stream.Collectors;

public class InlineJsonResult implements IExecutionResult {

  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(InlineJsonResult.class);
  private long currentRow;
  private List<List<Object>> data;
  private JsonChunkProvider chunkProvider;
  private boolean isClosed;
  private final IDatabricksSession session;
  private final List<ColumnInfo> columnInfos;

  public InlineJsonResult(
      ResultManifest resultManifest,
      ResultData resultData,
      StatementId statementId,
      IDatabricksSession session)
      throws DatabricksSQLException {

    this.session = session;
    this.columnInfos =
        resultManifest.getSchema().getColumnCount() == 0
            ? new ArrayList<>()
            : new ArrayList<>(resultManifest.getSchema().getColumns());
    this.chunkProvider = new JsonChunkProvider(resultManifest, resultData, statementId, session);
    // Fetching data all at once as the data is at most 26Mb in total (SEA)
    this.data = chunkProvider.getAllData();

    this.currentRow = -1;
    this.isClosed = false;
  }

  public InlineJsonResult(Object[][] rows) {
    this(
        Arrays.stream(rows)
            .map(row -> Arrays.stream(row).collect(Collectors.toList()))
            .collect(Collectors.toList()));
  }

  public InlineJsonResult(List<List<Object>> rows) {
    this.session = null;
    this.columnInfos = new ArrayList<>();
    this.data = rows.stream().map(ArrayList::new).collect(Collectors.toList());
    this.currentRow = -1;
    this.isClosed = false;
  }

  @Override
  public Object getObject(int columnIndex) throws DatabricksSQLException {
    if (isClosed()) {
      throw new DatabricksSQLException(
          "Result is already closed", DatabricksDriverErrorCode.STATEMENT_CLOSED);
    }
    if (currentRow == -1) {
      throw new DatabricksSQLException(
          "Cursor is before first row", DatabricksDriverErrorCode.INVALID_STATE);
    }
    if (columnIndex >= data.get((int) currentRow).size()) {
      throw new DatabricksSQLException(
          "Column index out of bounds " + columnIndex, DatabricksDriverErrorCode.INVALID_STATE);
    }

    Object result = data.get((int) currentRow).get(columnIndex);

    // Check if we need to handle geospatial types when support is disabled
    if (session != null && !columnInfos.isEmpty() && columnIndex < columnInfos.size()) {
      ColumnInfo columnInfo = columnInfos.get(columnIndex);
      ColumnInfoTypeName typeName = columnInfo.getTypeName();

      boolean isGeoSpatialSupportEnabled =
          session.getConnectionContext().isGeoSpatialSupportEnabled();

      // If geospatial support is disabled and this is a geospatial column, return as-is
      // JSON format already stores geospatial data as strings, so no conversion needed
      if (!isGeoSpatialSupportEnabled && isGeospatialType(typeName)) {
        LOGGER.debug(
            "Geospatial support is disabled for JSON format, returning {} as STRING", typeName);
        // Result is already a string in JSON format, return as-is
      }
    }

    return result;
  }

  @Override
  public long getCurrentRow() {
    return currentRow;
  }

  @Override
  public boolean next() {
    if (hasNext()) {
      currentRow++;
      return true;
    }
    return false;
  }

  @Override
  public boolean hasNext() {
    return !isClosed() && currentRow < data.size() - 1;
  }

  @Override
  public void close() {
    this.isClosed = true;
    this.data = null;
    if (chunkProvider != null) {
      chunkProvider.close();
    }
  }

  @Override
  public long getRowCount() {
    return data.size();
  }

  @Override
  public long getChunkCount() {
    return chunkProvider.getChunkCount();
  }

  private boolean isClosed() {
    return isClosed;
  }

  private boolean isGeospatialType(ColumnInfoTypeName type) {
    return type == ColumnInfoTypeName.GEOMETRY || type == ColumnInfoTypeName.GEOGRAPHY;
  }
}
