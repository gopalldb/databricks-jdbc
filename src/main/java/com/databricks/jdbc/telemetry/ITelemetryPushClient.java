package com.databricks.jdbc.telemetry;

import com.databricks.jdbc.model.telemetry.TelemetryRequest;

/**
 * Interface for pushing telemetry events. Implementations should handle the actual transmission of
 * telemetry data.
 */
interface ITelemetryPushClient {

  /**
   * Pushes a telemetry request to the telemetry service.
   *
   * @param request The telemetry request to be sent.
   * @throws Exception If there is an error while pushing the event.
   */
  void pushEvent(TelemetryRequest request) throws Exception;
}
