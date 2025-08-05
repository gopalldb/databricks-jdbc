package com.databricks.jdbc.telemetry;

import com.databricks.jdbc.model.telemetry.TelemetryRequest;

/**
 * Interface for pushing telemetry events. Implementations should handle the actual transmission of
 * telemetry data.
 */
interface ITelemetryPushClient {
  void pushEvent(TelemetryRequest request) throws Exception;
}
