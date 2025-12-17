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
   * <p>Matches exact parameter names from DatabricksJdbcUrlParams that contain sensitive data:
   *
   * <ul>
   *   <li>password, pwd - Authentication passwords
   *   <li>OAuth2Secret - OAuth2 client secret
   *   <li>Auth_AccessToken - OAuth2 access token
   *   <li>Auth_RefreshToken, OAuthRefreshToken - OAuth2 refresh tokens
   *   <li>proxyuid, cfproxyuid - Proxy usernames (usernames are sensitive in proxy context)
   *   <li>proxypwd, cfproxypwd - Proxy passwords
   *   <li>Auth_JWT_Key_Passphrase - JWT key passphrase
   *   <li>SSLTrustStorePwd - SSL trust store password
   *   <li>SSLKeyStorePwd - SSL key store password
   *   <li>TokenCachePassPhrase - Token cache passphrase
   * </ul>
   *
   * <p>Note: UID is not redacted as it's an identifier (e.g., email, username), not a secret like a
   * password or token.
   *
   * <p>Pattern matches: - Parameter name (case-insensitive) - Equals sign - Value (everything until
   * semicolon, ampersand, or end of string)
   */
  private static final Pattern CREDENTIAL_PATTERN =
      Pattern.compile(
          "(password|pwd"
              + "|OAuth2Secret"
              + "|Auth_AccessToken"
              + "|Auth_RefreshToken|OAuthRefreshToken"
              + "|proxyuid|proxypwd"
              + "|cfproxyuid|cfproxypwd"
              + "|Auth_JWT_Key_Passphrase"
              + "|SSLTrustStorePwd|SSLKeyStorePwd"
              + "|TokenCachePassPhrase"
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

  private SecurityUtil() {
    // Utility class, prevent instantiation
  }
}
