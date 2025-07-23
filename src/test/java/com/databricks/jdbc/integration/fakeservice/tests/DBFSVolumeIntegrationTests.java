package com.databricks.jdbc.integration.fakeservice.tests;

import static com.databricks.jdbc.TestConstants.UC_VOLUME_CATALOG;
import static com.databricks.jdbc.TestConstants.UC_VOLUME_SCHEMA;
import static com.databricks.jdbc.integration.IntegrationTestUtil.*;
import static com.databricks.jdbc.integration.IntegrationTestUtil.getDatabricksUser;
import static com.databricks.jdbc.integration.fakeservice.FakeServiceConfigLoader.PRESIGNED_URL_HOST;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.databricks.jdbc.api.IDatabricksVolumeClient;
import com.databricks.jdbc.api.impl.DatabricksConnectionContextFactory;
import com.databricks.jdbc.api.impl.VolumeOperationStatus;
import com.databricks.jdbc.api.impl.volume.DatabricksVolumeClientFactory;
import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.integration.fakeservice.AbstractFakeServiceIntegrationTests;
import com.databricks.jdbc.integration.fakeservice.FakeServiceConfigLoader;
import com.databricks.jdbc.model.client.filesystem.VolumePutResult;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class DBFSVolumeIntegrationTests extends AbstractFakeServiceIntegrationTests {
  private IDatabricksVolumeClient client;
  private IDatabricksConnectionContext connectionContext;
  private static final String jdbcUrlTemplate =
      "jdbc:databricks://%s/default;ssl=0;AuthMech=3;httpPath=%s;VolumeOperationAllowedLocalPaths=/tmp";

  @BeforeAll
  static void setupAll() throws Exception {
    setCloudFetchApiTargetUrl(FakeServiceConfigLoader.getProperty(PRESIGNED_URL_HOST));
  }

  @BeforeEach
  void setUp() throws SQLException {
    connectionContext = getConnectionContext();
    client = DatabricksVolumeClientFactory.getVolumeClient(connectionContext);
  }

  @ParameterizedTest
  @MethodSource("provideParametersForListObjectsInSubFolders")
  void testListObjects_SubFolders(
      String catalog,
      String schema,
      String volume,
      String prefix,
      boolean caseSensitive,
      List<String> expected)
      throws Exception {
    assertEquals(expected, client.listObjects(catalog, schema, volume, prefix, caseSensitive));
  }

  private static Stream<Arguments> provideParametersForListObjectsInSubFolders() {
    return Stream.of(
        Arguments.of(
            UC_VOLUME_CATALOG,
            UC_VOLUME_SCHEMA,
            "test_volume1",
            "#",
            true,
            Arrays.asList("#!#_file1.csv", "#!#_file3.csv", "#!_file3.csv")),
        Arguments.of(
            UC_VOLUME_CATALOG,
            UC_VOLUME_SCHEMA,
            "test_volume1",
            "folder1/a",
            true,
            Arrays.asList("aBc_file1.csv", "abc_file2.csv")),
        Arguments.of(
            UC_VOLUME_CATALOG,
            UC_VOLUME_SCHEMA,
            "test_volume1",
            "folder1/folder2/efg",
            true,
            Arrays.asList("efg_file1.csv")));
  }

  @ParameterizedTest
  @MethodSource("provideParametersForListObjectsVolumeReferencing")
  void testListObjects_VolumeReferencing(
      String catalog,
      String schema,
      String volume,
      String prefix,
      boolean caseSensitive,
      List<String> expected)
      throws Exception {
    assertEquals(expected, client.listObjects(catalog, schema, volume, prefix, caseSensitive));
  }

  private static Stream<Arguments> provideParametersForListObjectsVolumeReferencing() {
    return Stream.of(
        Arguments.of(
            UC_VOLUME_CATALOG,
            UC_VOLUME_SCHEMA,
            "test_volume1",
            "#",
            true,
            Arrays.asList("#!#_file1.csv", "#!#_file3.csv", "#!_file3.csv")),
        Arguments.of(
            UC_VOLUME_CATALOG,
            UC_VOLUME_SCHEMA,
            "test_volume2",
            "a",
            true,
            Arrays.asList("aBC_file3.csv", "abc_file2.csv", "abc_file4.csv")));
  }

  @ParameterizedTest
  @MethodSource("provideParametersForListObjectsCaseSensitivity_SpecialCharacters")
  void testListObjects_CaseSensitivity_SpecialCharacters(
      String catalog,
      String schema,
      String volume,
      String prefix,
      boolean caseSensitive,
      List<String> expected)
      throws Exception {
    assertEquals(expected, client.listObjects(catalog, schema, volume, prefix, caseSensitive));
  }

  private static Stream<Arguments>
      provideParametersForListObjectsCaseSensitivity_SpecialCharacters() {
    return Stream.of(
        Arguments.of(
            UC_VOLUME_CATALOG,
            UC_VOLUME_SCHEMA,
            "test_volume1",
            "#",
            true,
            Arrays.asList("#!#_file1.csv", "#!#_file3.csv", "#!_file3.csv")),
        Arguments.of(
            UC_VOLUME_CATALOG,
            UC_VOLUME_SCHEMA,
            "test_volume2",
            "ab",
            true,
            Arrays.asList("abc_file2.csv", "abc_file4.csv")),
        Arguments.of(
            UC_VOLUME_CATALOG,
            UC_VOLUME_SCHEMA,
            "test_volume2",
            "aB",
            true,
            Arrays.asList("aBC_file3.csv")),
        Arguments.of(
            UC_VOLUME_CATALOG,
            UC_VOLUME_SCHEMA,
            "test_volume2",
            "ab",
            false,
            Arrays.asList("aBC_file3.csv", "abc_file2.csv", "abc_file4.csv")));
  }

  @ParameterizedTest
  @MethodSource("provideParametersForGetObject")
  void testGetObject(
      String catalog,
      String schema,
      String volume,
      String objectPath,
      String localPath,
      boolean expected)
      throws Exception {
    File file = new File(localPath);
    try {
      assertEquals(expected, client.getObject(catalog, schema, volume, objectPath, localPath));
    } catch (Exception e) {
      throw e;
    } finally {
      file.delete();
    }
  }

  private static Stream<Arguments> provideParametersForGetObject() {
    return Stream.of(
        Arguments.of(
            UC_VOLUME_CATALOG,
            UC_VOLUME_SCHEMA,
            "test_volume1",
            "abc_file1.csv",
            "/tmp/download1.csv",
            true),
        Arguments.of(
            UC_VOLUME_CATALOG,
            UC_VOLUME_SCHEMA,
            "test_volume1",
            "folder1/folder2/efg_file1.csv",
            "/tmp/download2.csv",
            true));
  }

  @ParameterizedTest
  @MethodSource("provideParametersForPutObject")
  void testPutObject(
      String catalog,
      String schema,
      String volume,
      String objectPath,
      String localPath,
      boolean toOverwrite,
      boolean expected)
      throws Exception {
    File file = new File(localPath);
    try {
      Files.writeString(file.toPath(), "test-put");
      assertEquals(
          expected, client.putObject(catalog, schema, volume, objectPath, localPath, toOverwrite));
    } catch (Exception e) {
      throw e;
    } finally {
      file.delete();
    }
  }

  private static Stream<Arguments> provideParametersForPutObject() {
    return Stream.of(
        Arguments.of(
            UC_VOLUME_CATALOG,
            UC_VOLUME_SCHEMA,
            "test_volume1",
            "upload1.csv",
            "/tmp/downloadtest.csv",
            false,
            true),
        Arguments.of(
            UC_VOLUME_CATALOG,
            UC_VOLUME_SCHEMA,
            "test_volume1",
            "folder1/folder2/upload2.csv",
            "/tmp/download2.csv",
            false,
            true));
  }

  @ParameterizedTest
  @MethodSource("provideParametersForPutAndGetTest")
  void testPutAndGet(
      String catalog,
      String schema,
      String volume,
      String objectPath,
      boolean toOverwrite,
      String localPathForUpload,
      String localPathForDownload,
      String expectedContent)
      throws Exception {
    File file = new File(localPathForUpload);
    File downloadedFile = new File(localPathForDownload);

    try {
      Files.writeString(file.toPath(), expectedContent);

      assertTrue(
          client.putObject(catalog, schema, volume, objectPath, localPathForUpload, toOverwrite));
      assertTrue(client.getObject(catalog, schema, volume, objectPath, localPathForDownload));

      String actualContent = Files.readString(downloadedFile.toPath(), StandardCharsets.UTF_8);
      assertEquals(expectedContent, actualContent);
    } catch (Exception e) {
      throw e;
    } finally {
      file.delete();
      downloadedFile.delete();
    }
  }

  private static Stream<Arguments> provideParametersForPutAndGetTest() {
    return Stream.of(
        Arguments.of(
            UC_VOLUME_CATALOG,
            UC_VOLUME_SCHEMA,
            "test_volume1",
            "hello_world.txt",
            false,
            "/tmp/upload_hello_world.txt",
            "/tmp/download_hello_world.txt",
            "helloworld"));
  }

  @ParameterizedTest
  @MethodSource("provideParametersForPutAndDeleteTest")
  void testPutAndDelete(
      String catalog,
      String schema,
      String volume,
      String objectPath,
      String localPathForUpload,
      String fileContent)
      throws Exception {

    File file = new File(localPathForUpload);
    try {
      Files.writeString(file.toPath(), fileContent);

      assertTrue(client.putObject(catalog, schema, volume, objectPath, localPathForUpload, false));
      assertTrue(client.deleteObject(catalog, schema, volume, objectPath));
    } catch (Exception e) {
      throw e;
    } finally {
      file.delete();
    }
  }

  private static Stream<Arguments> provideParametersForPutAndDeleteTest() {
    return Stream.of(
        Arguments.of(
            UC_VOLUME_CATALOG,
            UC_VOLUME_SCHEMA,
            "test_volume1",
            "hello_world.txt",
            "/tmp/upload_hello_world.txt",
            "helloworld"));
  }

  @ParameterizedTest
  @MethodSource("provideParametersForPrefixExists")
  void testPrefixExists(
      String catalog,
      String schema,
      String volume,
      String prefix,
      boolean caseSensitive,
      boolean expected)
      throws Exception {
    boolean result = client.prefixExists(catalog, schema, volume, prefix, caseSensitive);
    assertEquals(expected, result);
  }

  private static Stream<Arguments> provideParametersForPrefixExists() {
    return Stream.of(
        // Empty prefix should return false.
        Arguments.of(UC_VOLUME_CATALOG, UC_VOLUME_SCHEMA, "test_volume1", "", true, false),
        // Basic matching prefix should return true.
        Arguments.of(UC_VOLUME_CATALOG, UC_VOLUME_SCHEMA, "test_volume1", "abc", true, true),
        // Non-matching prefix should return false.
        Arguments.of(UC_VOLUME_CATALOG, UC_VOLUME_SCHEMA, "test_volume1", "xyz", true, false),
        // Case-insensitive: "dEf" should match a file with "DEF" (assuming such file exists).
        Arguments.of(UC_VOLUME_CATALOG, UC_VOLUME_SCHEMA, "test_volume1", "dEf", false, true),
        // Special characters: prefix "#!" should be found.
        Arguments.of(UC_VOLUME_CATALOG, UC_VOLUME_SCHEMA, "test_volume1", "#!", true, true),
        // Mixed-case prefix: "aBc" matching exactly.
        Arguments.of(UC_VOLUME_CATALOG, UC_VOLUME_SCHEMA, "test_volume1", "aBc", true, true),
        // Prefix within a subfolder.
        Arguments.of(UC_VOLUME_CATALOG, UC_VOLUME_SCHEMA, "test_volume1", "folder1/ab", true, true),
        // Deep nested prefix found.
        Arguments.of(
            UC_VOLUME_CATALOG, UC_VOLUME_SCHEMA, "test_volume1", "folder1/folder2/e", true, true),
        // Deep nested prefix not found.
        Arguments.of(
            UC_VOLUME_CATALOG,
            UC_VOLUME_SCHEMA,
            "test_volume1",
            "folder1/folder2/xyz",
            true,
            false));
  }

  @ParameterizedTest
  @MethodSource("provideParametersForObjectExists")
  void testObjectExists(
      String catalog,
      String schema,
      String volume,
      String objectPath,
      boolean caseSensitive,
      boolean expected)
      throws Exception {
    boolean result = client.objectExists(catalog, schema, volume, objectPath, caseSensitive);
    assertEquals(expected, result);
  }

  private static Stream<Arguments> provideParametersForObjectExists() {
    return Stream.of(
        // Case-sensitive: if file "def_file1.csv" exists but "DeF_file1.csv" is requested, result
        // is false.
        Arguments.of(
            UC_VOLUME_CATALOG, UC_VOLUME_SCHEMA, "test_volume1", "DeF_file1.csv", true, false),
        // Case-sensitive: matching case returns true.
        Arguments.of(
            UC_VOLUME_CATALOG, UC_VOLUME_SCHEMA, "test_volume1", "aBc_file1.csv", true, true),
        // Case-insensitive: "abc_file1.csv" matches "aBc_file1.csv".
        Arguments.of(
            UC_VOLUME_CATALOG, UC_VOLUME_SCHEMA, "test_volume1", "abc_file1.csv", false, true),
        // Object in a subfolder (case-insensitive match).
        Arguments.of(
            UC_VOLUME_CATALOG,
            UC_VOLUME_SCHEMA,
            "test_volume1",
            "folder1/ABC_file1.csv",
            false,
            true),
        // Nested folder: exact match found.
        Arguments.of(
            UC_VOLUME_CATALOG,
            UC_VOLUME_SCHEMA,
            "test_volume1",
            "folder1/folder2/efg_file1.csv",
            true,
            true),
        // No match.
        Arguments.of(
            UC_VOLUME_CATALOG,
            UC_VOLUME_SCHEMA,
            "test_volume1",
            "folder1/folder2/xyz_file.csv",
            true,
            false));
  }

  @ParameterizedTest
  @MethodSource("provideParametersForVolumeExists")
  void testVolumeExists(
      String catalog, String schema, String volumeName, boolean caseSensitive, boolean expected)
      throws Exception {
    boolean result = client.volumeExists(catalog, schema, volumeName, caseSensitive);
    assertEquals(expected, result);
  }

  private static Stream<Arguments> provideParametersForVolumeExists() {
    return Stream.of(
        // Expected volume exists.
        Arguments.of(UC_VOLUME_CATALOG, UC_VOLUME_SCHEMA, "test_volume1", true, true),
        // Volume with special characters.
        Arguments.of(UC_VOLUME_CATALOG, UC_VOLUME_SCHEMA, "###", true, true),
        // Non-existent volume.
        Arguments.of(UC_VOLUME_CATALOG, UC_VOLUME_SCHEMA, "test_volume5", true, false));
  }

  @ParameterizedTest
  @MethodSource("provideParametersForMultiFilePutTest")
  void testPutFiles(
      String catalog,
      String schema,
      String volume,
      List<String> objectPaths,
      List<String> inputLocalPaths,
      List<Boolean> expectedSuccessFlags)
      throws Exception {
    List<String> localPaths = new ArrayList<>();

    try {
      // Create temporary files with contents
      for (int i = 0; i < inputLocalPaths.size(); i++) {
        String tempPath = "/tmp/multi_upload_" + i + ".txt";
        File file = new File(tempPath);
        Files.writeString(file.toPath(), "test-content-" + i);
        localPaths.add(tempPath);
      }

      // Upload all files in a single operation
      List<VolumePutResult> results =
          client.putFiles(catalog, schema, volume, objectPaths, inputLocalPaths, true);

      // Verify results
      assertEquals(
          objectPaths.size(), results.size(), "Results count should match input files count");

      // Check that results have expected status
      for (int i = 0; i < results.size(); i++) {
        VolumePutResult result = results.get(i);
        assertEquals(
            expectedSuccessFlags.get(i)
                ? VolumeOperationStatus.SUCCEEDED
                : VolumeOperationStatus.FAILED,
            result.getStatus(),
            "Status for file at index "
                + i
                + " ("
                + objectPaths.get(i)
                + ") should match expected");
      }

      // Verify successful uploads actually worked by checking file existence
      for (int i = 0; i < objectPaths.size(); i++) {
        if (expectedSuccessFlags.get(i)) {
          assertTrue(
              client.objectExists(catalog, schema, volume, objectPaths.get(i), true),
              "File should exist: " + objectPaths.get(i));
        }
      }
    } finally {
      // Clean up temporary files
      for (String path : localPaths) {
        new File(path).delete();
      }

      // Clean up uploaded files
      for (int i = 0; i < objectPaths.size(); i++) {
        if (expectedSuccessFlags.get(i)) {
          client.deleteObject(catalog, schema, volume, objectPaths.get(i));
        }
      }
    }
  }

  private static Stream<Arguments> provideParametersForMultiFilePutTest() {
    return Stream.of(
        // Test case 1: All files succeed
        Arguments.of(
            UC_VOLUME_CATALOG,
            UC_VOLUME_SCHEMA,
            "test_volume1",
            Arrays.asList("multi_upload_1.txt", "multi_upload_2.txt", "multi_upload_3.txt"),
            Arrays.asList(
                "/tmp/multi_upload_0.txt", "/tmp/multi_upload_1.txt", "/tmp/multi_upload_2.txt"),
            Arrays.asList(true, true, true)),

        // Test case 2: Mixed success/failure - missing local file
        Arguments.of(
            UC_VOLUME_CATALOG,
            UC_VOLUME_SCHEMA,
            "test_volume1",
            Arrays.asList("multi_upload_4.txt", "missing_file.txt", "multi_upload_5.txt"),
            Arrays.asList(
                "/tmp/multi_upload_0.txt", "/tmp/multi_upload_100.txt", "/tmp/multi_upload_2.txt"),
            Arrays.asList(true, false, true)));
  }

  @ParameterizedTest
  @MethodSource("provideParametersForMultiStreamPutTest")
  void testPutFilesFromStreams(
      String catalog,
      String schema,
      String volume,
      List<String> objectPaths,
      List<String> streamContents,
      List<Boolean> expectedSuccessFlags)
      throws Exception {

    List<InputStream> inputStreams = new ArrayList<>();
    List<Long> contentLengths = new ArrayList<>();

    try {
      // Create input streams with contents
      for (String content : streamContents) {
        byte[] bytes = content.getBytes(StandardCharsets.UTF_8);
        inputStreams.add(new ByteArrayInputStream(bytes));
        contentLengths.add((long) bytes.length);
      }

      // Upload all streams in a single operation
      List<VolumePutResult> results =
          client.putFiles(
              catalog, schema, volume, objectPaths, inputStreams, contentLengths, false);

      // Verify results
      assertEquals(
          objectPaths.size(), results.size(), "Results count should match input streams count");

      // Check that results have expected status
      for (int i = 0; i < results.size(); i++) {
        VolumePutResult result = results.get(i);
        assertEquals(
            expectedSuccessFlags.get(i)
                ? VolumeOperationStatus.SUCCEEDED
                : VolumeOperationStatus.FAILED,
            result.getStatus(),
            "Status for file at index "
                + i
                + " ("
                + objectPaths.get(i)
                + ") should match expected");
      }

      // Verify successful uploads by downloading and checking content
      for (int i = 0; i < objectPaths.size(); i++) {
        if (expectedSuccessFlags.get(i)) {
          String downloadPath = "/tmp/multi_download_" + i + ".txt";
          client.getObject(catalog, schema, volume, objectPaths.get(i), downloadPath);
          String downloadedContent = Files.readString(new File(downloadPath).toPath());
          assertEquals(
              streamContents.get(i),
              downloadedContent,
              "Downloaded content should match uploaded content");
          new File(downloadPath).delete();
        }
      }
    } finally {
      // Close input streams
      for (InputStream stream : inputStreams) {
        stream.close();
      }

      // Clean up uploaded files
      for (int i = 0; i < objectPaths.size(); i++) {
        if (expectedSuccessFlags.get(i)) {
          client.deleteObject(catalog, schema, volume, objectPaths.get(i));
        }
      }
    }
  }

  private static Stream<Arguments> provideParametersForMultiStreamPutTest() {
    return Stream.of(
        // Test case for streams: All succeed
        Arguments.of(
            UC_VOLUME_CATALOG,
            UC_VOLUME_SCHEMA,
            "test_volume1",
            Arrays.asList("stream_upload_1.txt", "stream_upload_2.txt", "stream_upload_3.txt"),
            Arrays.asList("stream-content1", "stream-content2", "stream-content3"),
            Arrays.asList(true, true, true)));
  }

  private IDatabricksConnectionContext getConnectionContext() throws SQLException {
    String jdbcUrl = String.format(jdbcUrlTemplate, getFakeServiceHost(), getFakeServiceHTTPPath());
    return DatabricksConnectionContextFactory.create(
        jdbcUrl, getDatabricksUser(), getDatabricksToken());
  }
}
