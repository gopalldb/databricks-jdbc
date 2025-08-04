package com.databricks.jdbc.common.util;

import static com.databricks.jdbc.common.DatabricksJdbcConstants.*;

import com.databricks.jdbc.common.DatabricksJdbcUrlParams;
import com.databricks.jdbc.exception.DatabricksHttpException;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.exception.DatabricksValidationException;
import com.databricks.jdbc.exception.DatabricksVendorCode;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.http.HttpResponse;
import org.apache.http.util.EntityUtils;

public class ValidationUtil {

  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(ValidationUtil.class);

  public static <T extends Number> void checkIfNonNegative(T number, String fieldName)
      throws DatabricksSQLException {
    if (number.longValue() < 0) {
      String errorMessage =
          String.format("Invalid input for %s, : %d", fieldName, number.longValue());
      LOGGER.error(errorMessage);
      throw new DatabricksValidationException(errorMessage);
    }
  }

  public static void throwErrorIfNull(Map<String, String> fields, String context)
      throws DatabricksSQLException {
    for (Map.Entry<String, String> field : fields.entrySet()) {
      if (field.getValue() == null) {
        String errorMessage =
            String.format(
                "Unsupported null Input for field {%s}. Context: {%s}", field.getKey(), context);
        LOGGER.error(errorMessage);
        throw new DatabricksValidationException(errorMessage);
      }
    }
  }

  public static void throwErrorIfNull(String field, Object value) throws DatabricksSQLException {
    if (value != null) {
      return;
    }
    String errorMessage = String.format("Unsupported null Input for field {%s}", field);
    LOGGER.error(errorMessage);
    throw new DatabricksValidationException(errorMessage);
  }

  public static void checkHTTPError(HttpResponse response)
      throws DatabricksHttpException, IOException {
    int statusCode = response.getStatusLine().getStatusCode();
    String statusLine = response.getStatusLine().toString();
    if (statusCode >= 200 && statusCode < 300) {
      return;
    }
    String errorReason =
        String.format("HTTP request failed by code: %d, status line: %s.", statusCode, statusLine);
    if (response.containsHeader(THRIFT_ERROR_MESSAGE_HEADER)) {
      errorReason +=
          String.format(
              " Thrift Header : %s",
              response.getFirstHeader(THRIFT_ERROR_MESSAGE_HEADER).getValue());
    }
    if (response.getEntity() != null) {
      try {
        JsonNode jsonNode =
            JsonUtil.getMapper().readTree(EntityUtils.toString(response.getEntity()));
        JsonNode errorNode = jsonNode.path("message");
        if (errorNode.isTextual()) {
          errorReason += String.format(" Error message: %s", errorNode.textValue());
        }
      } catch (Exception e) {
        LOGGER.warn("Unable to parse JSON from response entity", e);
      }
    }

    LOGGER.error(errorReason);
    throw new DatabricksHttpException(errorReason, DEFAULT_HTTP_EXCEPTION_SQLSTATE);
  }

  /**
   * Validates the JDBC URL.
   *
   * @param url JDBC URL
   * @return true if the URL is valid, false otherwise
   */
  public static boolean isValidJdbcUrl(String url) {
    final List<Pattern> PATH_PATTERNS =
        List.of(
            HTTP_CLUSTER_PATH_PATTERN,
            HTTP_WAREHOUSE_PATH_PATTERN,
            HTTP_ENDPOINT_PATH_PATTERN,
            TEST_PATH_PATTERN,
            BASE_PATTERN,
            HTTP_CLI_PATTERN);

    // check if URL matches the generic format
    if (!JDBC_URL_PATTERN.matcher(url).matches()) {
      return false;
    }

    // check if path in URL matches any of the specific patterns
    return PATH_PATTERNS.stream().anyMatch(pattern -> pattern.matcher(url).matches());
  }

  /**
   * Validates all input properties for JDBC connection parameters. This method serves as a central
   * validation entry point, consolidating all property validations in one place for better
   * maintainability and extensibility.
   *
   * @param parameters Map of JDBC connection parameters to validate
   * @throws DatabricksSQLException if any validation fails
   */
  public static void validateInputProperties(Map<String, String> parameters)
      throws DatabricksSQLException {
    // Validate UID parameter
    validateUidParameter(parameters);

    // Future property validations can be added here
  }

  /**
   * Validates the UID parameter in JDBC connection properties. UID must either be omitted or set to
   * "token".
   *
   * @param parameters Map of JDBC connection parameters
   * @throws DatabricksSQLException if UID validation fails
   */
  public static void validateUidParameter(Map<String, String> parameters)
      throws DatabricksSQLException {
    String uid = parameters.get(DatabricksJdbcUrlParams.UID.getParamName());
    // UID must either be omitted or set to "token"
    if (uid != null && !uid.equals(VALID_UID_VALUE)) {
      LOGGER.error(DatabricksVendorCode.INCORRECT_UID.getMessage());
      throw new DatabricksValidationException(
          DatabricksVendorCode.INCORRECT_UID.getMessage(),
          DatabricksVendorCode.INCORRECT_UID.getCode());
    }
  }
}
