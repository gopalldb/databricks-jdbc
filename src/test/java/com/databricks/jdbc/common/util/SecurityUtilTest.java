package com.databricks.jdbc.common.util;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

/** Unit tests for SecurityUtil class. */
public class SecurityUtilTest {

  @Test
  void testSanitizeJdbcUrl_withPassword() {
    String url = "jdbc:databricks://host:443/default;PWD=secret123;HttpPath=/sql/1.0";
    String sanitized = SecurityUtil.sanitizeJdbcUrl(url);

    assertNotNull(sanitized);
    assertFalse(sanitized.contains("secret123"), "Sanitized URL should not contain the password");
    assertTrue(
        sanitized.contains("PWD=***REDACTED***"),
        "Sanitized URL should contain redacted password placeholder");
    assertTrue(
        sanitized.contains("HttpPath=/sql/1.0"), "Non-sensitive parameters should be preserved");
  }

  @Test
  void testSanitizeJdbcUrl_withUid() {
    String url = "jdbc:databricks://host:443/default;UID=user@example.com;HttpPath=/sql/1.0";
    String sanitized = SecurityUtil.sanitizeJdbcUrl(url);

    assertNotNull(sanitized);
    // UID is not redacted (similar to CLIENT_ID, it's an identifier not a secret)
    assertTrue(sanitized.contains("user@example.com"), "UID should be preserved in sanitized URL");
    assertTrue(sanitized.contains("UID=user@example.com"), "UID parameter should be visible");
  }

  @Test
  void testSanitizeJdbcUrl_withAuthAccessToken() {
    String url =
        "jdbc:databricks://host:443/default;Auth_AccessToken=dapi1234567890abcdef;HttpPath=/sql/1.0";
    String sanitized = SecurityUtil.sanitizeJdbcUrl(url);

    assertNotNull(sanitized);
    assertFalse(
        sanitized.contains("dapi1234567890abcdef"), "Sanitized URL should not contain the token");
    assertTrue(
        sanitized.contains("Auth_AccessToken=***REDACTED***"),
        "Sanitized URL should contain redacted token placeholder");
  }

  @Test
  void testSanitizeJdbcUrl_withClientSecret() {
    String url = "jdbc:databricks://host:443/default;OAuth2Secret=myclientsecret;HttpPath=/sql/1.0";
    String sanitized = SecurityUtil.sanitizeJdbcUrl(url);

    assertNotNull(sanitized);
    assertFalse(
        sanitized.contains("myclientsecret"), "Sanitized URL should not contain the client secret");
    assertTrue(
        sanitized.contains("OAuth2Secret=***REDACTED***"),
        "Sanitized URL should contain redacted client secret placeholder");
  }

  @Test
  void testSanitizeJdbcUrl_withOAuthRefreshToken() {
    String url =
        "jdbc:databricks://host:443/default;Auth_RefreshToken=refresh_token_abc123;HttpPath=/sql/1.0";
    String sanitized = SecurityUtil.sanitizeJdbcUrl(url);

    assertNotNull(sanitized);
    assertFalse(
        sanitized.contains("refresh_token_abc123"),
        "Sanitized URL should not contain the refresh token");
    assertTrue(
        sanitized.contains("Auth_RefreshToken=***REDACTED***"),
        "Sanitized URL should contain redacted refresh token placeholder");
  }

  @Test
  void testSanitizeJdbcUrl_withProxyPassword() {
    String url =
        "jdbc:databricks://host:443/default;ProxyPwd=proxypass123;ProxyUID=proxyuser;HttpPath=/sql/1.0";
    String sanitized = SecurityUtil.sanitizeJdbcUrl(url);

    assertNotNull(sanitized);
    assertFalse(
        sanitized.contains("proxypass123"), "Sanitized URL should not contain the proxy password");
    assertFalse(sanitized.contains("proxyuser"), "Sanitized URL should not contain the proxy user");
    assertTrue(
        sanitized.contains("ProxyPwd=***REDACTED***"),
        "Sanitized URL should contain redacted proxy password placeholder");
    assertTrue(
        sanitized.contains("ProxyUID=***REDACTED***"),
        "Sanitized URL should contain redacted proxy user placeholder");
  }

  @Test
  void testSanitizeJdbcUrl_withCloudFetchProxyPassword() {
    String url =
        "jdbc:databricks://host:443/default;CFProxyPwd=cfproxypass;CFProxyUID=cfproxyuser;HttpPath=/sql/1.0";
    String sanitized = SecurityUtil.sanitizeJdbcUrl(url);

    assertNotNull(sanitized);
    assertFalse(
        sanitized.contains("cfproxypass"),
        "Sanitized URL should not contain the CloudFetch proxy password");
    assertFalse(
        sanitized.contains("cfproxyuser"),
        "Sanitized URL should not contain the CloudFetch proxy user");
  }

  @Test
  void testSanitizeJdbcUrl_withJwtPassPhrase() {
    String url =
        "jdbc:databricks://host:443/default;Auth_JWT_Key_Passphrase=mypassphrase;HttpPath=/sql/1.0";
    String sanitized = SecurityUtil.sanitizeJdbcUrl(url);

    assertNotNull(sanitized);
    assertFalse(
        sanitized.contains("mypassphrase"), "Sanitized URL should not contain the JWT passphrase");
    assertTrue(
        sanitized.contains("Auth_JWT_Key_Passphrase=***REDACTED***"),
        "Sanitized URL should contain redacted JWT passphrase placeholder");
  }

  @Test
  void testSanitizeJdbcUrl_withSslTrustStorePassword() {
    String url = "jdbc:databricks://host:443/default;SSLTrustStorePwd=changeit;HttpPath=/sql/1.0";
    String sanitized = SecurityUtil.sanitizeJdbcUrl(url);

    assertNotNull(sanitized);
    assertFalse(
        sanitized.contains("changeit"),
        "Sanitized URL should not contain the SSL trust store password");
    assertTrue(
        sanitized.contains("SSLTrustStorePwd=***REDACTED***"),
        "Sanitized URL should contain redacted SSL trust store password placeholder");
  }

  @Test
  void testSanitizeJdbcUrl_withSslKeyStorePassword() {
    String url = "jdbc:databricks://host:443/default;SSLKeyStorePwd=keystorepass;HttpPath=/sql/1.0";
    String sanitized = SecurityUtil.sanitizeJdbcUrl(url);

    assertNotNull(sanitized);
    assertFalse(
        sanitized.contains("keystorepass"),
        "Sanitized URL should not contain the SSL key store password");
    assertTrue(
        sanitized.contains("SSLKeyStorePwd=***REDACTED***"),
        "Sanitized URL should contain redacted SSL key store password placeholder");
  }

  @Test
  void testSanitizeJdbcUrl_withTokenCachePassPhrase() {
    String url =
        "jdbc:databricks://host:443/default;TokenCachePassPhrase=cachepass;HttpPath=/sql/1.0";
    String sanitized = SecurityUtil.sanitizeJdbcUrl(url);

    assertNotNull(sanitized);
    assertFalse(
        sanitized.contains("cachepass"),
        "Sanitized URL should not contain the token cache passphrase");
    assertTrue(
        sanitized.contains("TokenCachePassPhrase=***REDACTED***"),
        "Sanitized URL should contain redacted token cache passphrase placeholder");
  }

  @Test
  void testSanitizeJdbcUrl_withMultipleCredentials() {
    String url =
        "jdbc:databricks://host:443/default;PWD=secret123;UID=user@example.com;OAuth2Secret=clientsecret;HttpPath=/sql/1.0;LogLevel=DEBUG";
    String sanitized = SecurityUtil.sanitizeJdbcUrl(url);

    assertNotNull(sanitized);
    assertFalse(sanitized.contains("secret123"), "Sanitized URL should not contain the password");
    assertTrue(
        sanitized.contains("user@example.com"), "UID should be preserved (it's an identifier)");
    assertFalse(
        sanitized.contains("clientsecret"), "Sanitized URL should not contain the client secret");
    assertTrue(
        sanitized.contains("PWD=***REDACTED***"), "Sanitized URL should contain redacted password");
    assertTrue(sanitized.contains("UID=user@example.com"), "UID should be visible");
    assertTrue(
        sanitized.contains("OAuth2Secret=***REDACTED***"),
        "Sanitized URL should contain redacted client secret");
    assertTrue(
        sanitized.contains("HttpPath=/sql/1.0"),
        "Non-sensitive parameters should be preserved (HttpPath)");
    assertTrue(
        sanitized.contains("LogLevel=DEBUG"),
        "Non-sensitive parameters should be preserved (LogLevel)");
  }

  @Test
  void testSanitizeJdbcUrl_withNoCredentials() {
    String url =
        "jdbc:databricks://host:443/default;HttpPath=/sql/1.0;LogLevel=DEBUG;EnableArrow=1";
    String sanitized = SecurityUtil.sanitizeJdbcUrl(url);

    assertNotNull(sanitized);
    assertEquals(url, sanitized, "URL with no credentials should remain unchanged");
  }

  @Test
  void testSanitizeJdbcUrl_withNull() {
    String sanitized = SecurityUtil.sanitizeJdbcUrl(null);
    assertNull(sanitized, "Null input should return null");
  }

  @Test
  void testSanitizeJdbcUrl_caseInsensitive() {
    // Test with different case variations
    String url1 = "jdbc:databricks://host:443/default;pwd=secret123;HttpPath=/sql/1.0";
    String url2 = "jdbc:databricks://host:443/default;PWD=secret123;HttpPath=/sql/1.0";
    String url3 = "jdbc:databricks://host:443/default;PwD=secret123;HttpPath=/sql/1.0";

    String sanitized1 = SecurityUtil.sanitizeJdbcUrl(url1);
    String sanitized2 = SecurityUtil.sanitizeJdbcUrl(url2);
    String sanitized3 = SecurityUtil.sanitizeJdbcUrl(url3);

    // All variations should be sanitized
    assertFalse(sanitized1.contains("secret123"), "Lowercase pwd should be sanitized");
    assertFalse(sanitized2.contains("secret123"), "Uppercase PWD should be sanitized");
    assertFalse(sanitized3.contains("secret123"), "Mixed case PwD should be sanitized");
  }

  @Test
  void testSanitizeConnectionString() {
    String connectionString = "jdbc:databricks://host:443/default;PWD=secret123;HttpPath=/sql/1.0";
    String sanitized = SecurityUtil.sanitizeConnectionString(connectionString);

    assertNotNull(sanitized);
    assertFalse(
        sanitized.contains("secret123"),
        "sanitizeConnectionString should work the same as sanitizeJdbcUrl");
    assertTrue(sanitized.contains("PWD=***REDACTED***"));
  }

  @Test
  void testIsCredentialParameter() {
    // Test positive cases
    assertTrue(SecurityUtil.isCredentialParameter("PASSWORD"));
    assertTrue(SecurityUtil.isCredentialParameter("PWD"));
    assertTrue(SecurityUtil.isCredentialParameter("pwd"));
    assertTrue(SecurityUtil.isCredentialParameter("OAuth2Secret"));
    assertTrue(SecurityUtil.isCredentialParameter("AUTH_ACCESS_TOKEN"));
    assertTrue(SecurityUtil.isCredentialParameter("Auth_RefreshToken"));
    assertTrue(SecurityUtil.isCredentialParameter("JWT_PASS_PHRASE"));
    assertTrue(SecurityUtil.isCredentialParameter("SSLTrustStorePwd"));
    assertTrue(SecurityUtil.isCredentialParameter("TOKEN"));
    assertTrue(SecurityUtil.isCredentialParameter("SECRET"));
    assertTrue(SecurityUtil.isCredentialParameter("PASSPHRASE"));
    assertTrue(SecurityUtil.isCredentialParameter("ProxyUser"));
    assertTrue(SecurityUtil.isCredentialParameter("ProxyUID"));

    // Test negative cases
    assertFalse(SecurityUtil.isCredentialParameter("HttpPath"));
    assertFalse(SecurityUtil.isCredentialParameter("LogLevel"));
    assertFalse(SecurityUtil.isCredentialParameter("EnableArrow"));
    assertFalse(SecurityUtil.isCredentialParameter("HOST"));
    assertFalse(SecurityUtil.isCredentialParameter("UID")); // UID is an identifier, not a secret
    assertFalse(SecurityUtil.isCredentialParameter(null));
  }

  @Test
  void testSanitizeJdbcUrl_withAmpersandSeparator() {
    // Some JDBC URLs might use & as separator instead of ;
    String url = "jdbc:databricks://host:443/default?PWD=secret123&HttpPath=/sql/1.0&UID=user";
    String sanitized = SecurityUtil.sanitizeJdbcUrl(url);

    assertNotNull(sanitized);
    assertFalse(sanitized.contains("secret123"), "Sanitized URL should not contain the password");
    assertTrue(
        sanitized.contains("UID=user"), "UID should be preserved (it's an identifier, not secret)");
  }

  @Test
  void testSanitizeJdbcUrl_withEmptyPasswordValue() {
    String url = "jdbc:databricks://host:443/default;PWD=;HttpPath=/sql/1.0";
    String sanitized = SecurityUtil.sanitizeJdbcUrl(url);

    assertNotNull(sanitized);
    assertTrue(
        sanitized.contains("PWD=***REDACTED***"),
        "Even empty password should be redacted for consistency");
  }

  @Test
  void testSanitizeJdbcUrl_preservesStructure() {
    String url =
        "jdbc:databricks://host:443/default;AuthMech=3;PWD=secret;HttpPath=/sql/1.0;LogLevel=DEBUG;UID=user";
    String sanitized = SecurityUtil.sanitizeJdbcUrl(url);

    // Check that the URL structure is preserved
    assertTrue(sanitized.startsWith("jdbc:databricks://"));
    assertTrue(sanitized.contains("host:443"));
    assertTrue(sanitized.contains("AuthMech=3"));
    assertTrue(sanitized.contains("HttpPath=/sql/1.0"));
    assertTrue(sanitized.contains("LogLevel=DEBUG"));
    assertTrue(sanitized.contains("UID=user"), "UID should be preserved (identifier, not secret)");
    // But credentials are redacted
    assertFalse(sanitized.contains("secret"));
  }
}
