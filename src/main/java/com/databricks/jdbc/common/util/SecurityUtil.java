package com.databricks.jdbc.common.util;

import java.util.regex.Pattern;

/**
 * Security utility class for sanitizing sensitive information in logs and exception messages.
 *
 * <p>This class provides methods to redact credentials and other sensitive data from strings that
 * might be logged or included in error messages, preventing accidental exposure of secrets in log
 * files.
 */
public class SecurityUtil {

  /**
   * Pattern to match credential parameters in JDBC URLs.
   *
   * <p>Matches all parameter names from DatabricksJdbcUrlParams that contain sensitive data:
   *
   * <ul>
   *   <li>PASSWORD, PWD - Authentication passwords
   *   <li>CLIENT_SECRET, OAUTH2SECRET - OAuth2 client secrets
   *   <li>AUTH_ACCESS_TOKEN, AUTH_REFRESHTOKEN - OAuth2 tokens
   *   <li>OAUTHREFRESHTOKEN - OAuth2 refresh tokens
   *   <li>PROXY_PWD, PROXYPWD, CFPROXYPWD - Proxy passwords
   *   <li>PROXY_USER, PROXYUID, CFPROXYUID - Proxy usernames
   *   <li>JWT_PASS_PHRASE, AUTH_JWT_KEY_PASSPHRASE - JWT passphrases
   *   <li>SSL_TRUST_STORE_PASSWORD, SSLTRUSTTOREPWD - SSL trust store passwords
   *   <li>SSL_KEY_STORE_PASSWORD, SSLKEYSTOREPWD - SSL key store passwords
   *   <li>TOKEN_CACHE_PASS_PHRASE, TOKENCACHEPASSPHRASE - Token cache passphrases
   *   <li>UID - User identifier
   * </ul>
   *
   * <p>Pattern matches: - Parameter name (case-insensitive) - Equals sign - Value (everything until
   * semicolon, ampersand, or end of string)
   */
  private static final Pattern CREDENTIAL_PATTERN =
      Pattern.compile(
          "(PASSWORD|PWD"
              + "|CLIENT_?SECRET|OAUTH2SECRET"
              + "|AUTH_?ACCESS_?TOKEN"
              + "|AUTH_?REFRESH_?TOKEN|OAUTH_?REFRESH_?TOKEN"
              + "|PROXY_?PWD|CF_?PROXY_?PWD"
              + "|PROXY_?USER|PROXY_?UID|CF_?PROXY_?UID"
              + "|JWT_?PASS_?PHRASE|AUTH_?JWT_?KEY_?PASSPHRASE"
              + "|SSL_?TRUST_?STORE_?PASSWORD|SSL_?TRUST_?STORE_?PWD"
              + "|SSL_?KEY_?STORE_?PASSWORD|SSL_?KEY_?STORE_?PWD"
              + "|TOKEN_?CACHE_?PASS_?PHRASE"
              + "|UID"
              + ")=[^;&]*",
          Pattern.CASE_INSENSITIVE);

  /** Redaction string used to replace credential values. */
  private static final String REDACTED = "***REDACTED***";

  /**
   * Sanitizes a JDBC URL by redacting credential parameters.
   *
   * <p>This method should be used whenever a JDBC URL needs to be logged or included in an
   * exception message. It replaces the values of sensitive parameters with "***REDACTED***" while
   * preserving the parameter names to aid in debugging.
   *
   * <p>Example: Input: {@code
   * jdbc:databricks://host:443/default;PWD=secret123;UID=user@email.com;HttpPath=/sql/1.0} Output:
   * {@code
   * jdbc:databricks://host:443/default;PWD=***REDACTED***;UID=***REDACTED***;HttpPath=/sql/1.0}
   *
   * @param jdbcUrl the JDBC URL to sanitize, may be null
   * @return sanitized URL with credentials redacted, or null if input was null
   */
  public static String sanitizeJdbcUrl(String jdbcUrl) {
    if (jdbcUrl == null) {
      return null;
    }
    return CREDENTIAL_PATTERN.matcher(jdbcUrl).replaceAll("$1=" + REDACTED);
  }

  /**
   * Sanitizes a connection string by redacting credential parameters.
   *
   * <p>Alias for {@link #sanitizeJdbcUrl(String)} to support different naming conventions.
   *
   * @param connectionString the connection string to sanitize, may be null
   * @return sanitized connection string with credentials redacted, or null if input was null
   */
  public static String sanitizeConnectionString(String connectionString) {
    return sanitizeJdbcUrl(connectionString);
  }

  /**
   * Checks if a parameter name represents a credential or sensitive value.
   *
   * <p>This method can be used to determine whether a parameter should be redacted before logging.
   *
   * @param parameterName the parameter name to check, case-insensitive
   * @return true if the parameter represents a credential, false otherwise
   */
  public static boolean isCredentialParameter(String parameterName) {
    if (parameterName == null) {
      return false;
    }
    String upperName = parameterName.toUpperCase().replaceAll("[_\\s-]", "");
    return upperName.contains("PASSWORD")
        || upperName.contains("PWD")
        || upperName.equals("PWD")
        || upperName.contains("TOKEN")
        || upperName.contains("SECRET")
        || upperName.contains("PASSPHRASE")
        || upperName.equals("UID")
        || upperName.contains("PROXYUSER")
        || upperName.contains("PROXYUID");
  }

  private SecurityUtil() {
    // Utility class, prevent instantiation
  }
}
