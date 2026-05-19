package com.databricks.jdbc.integration.fakeservice.tests;

import static com.databricks.jdbc.integration.IntegrationTestUtil.getValidJDBCConnection;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.databricks.jdbc.integration.fakeservice.AbstractFakeServiceIntegrationTests;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import org.junit.jupiter.api.Test;

public class SqlExecApiIntegrationTests extends AbstractFakeServiceIntegrationTests {

  @Test
  void testChunkedResults_enableArrowDeprecatedIgnored() throws SQLException {
    // EnableArrow=0 is deprecated and ignored on non-AIX — Arrow is always enabled.
    // This test verifies the query still succeeds with the deprecated flag set.
    final String table = "samples.tpch.lineitem";
    final int maxRows = 64000;
    final String sql = "SELECT * FROM " + table + " limit " + maxRows;

    Properties properties = new Properties();
    properties.setProperty("EnableArrow", "0");
    Connection connection = getValidJDBCConnection(properties);

    final Statement stmt = connection.createStatement();
    ResultSet rs = stmt.executeQuery(sql);
    int rowCount = 0;
    while (rs.next()) {
      rowCount++;
    }
    assertEquals(maxRows, rowCount);
  }
}
