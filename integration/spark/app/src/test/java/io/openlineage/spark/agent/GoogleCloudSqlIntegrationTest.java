/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent;

import static org.assertj.core.api.Assertions.assertThat;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.openlineage.client.OpenLineage;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SparkSession$;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockserver.integration.ClientAndServer;
import org.testcontainers.containers.GenericContainer;

@Tag("integration-test")
// @Tag("google-cloud")
@Slf4j
// @EnabledIfEnvironmentVariable(named = "CI", matches = "true")
// TODO: Please note the test remains disabled for Spark 4.0 for now (no applicable connector
// version available)
class GoogleCloudSqlIntegrationTest {
  private static final String PROJECT_ID =
      Optional.ofNullable(System.getenv("GCLOUD_PROJECT_ID")).orElse("gcp-open-lineage-testing");

  private static final String REGION =
      Optional.ofNullable(System.getenv("GCLOUD_CLOUDSQL_REGION")).orElse("us-central1");

  private static final String INSTANCE_NAME =
      Optional.ofNullable(System.getenv("GCLOUD_CLOUDSQL_INSTANCE")).orElse("open-lineage-e2e");

  private static final String DB_USER =
      Optional.ofNullable(System.getenv("GCLOUD_CLOUDSQL_USER")).orElse("e2euser2");

  private static final String DB_PASSWORD =
      Optional.ofNullable(System.getenv("GCLOUD_CLOUDSQL_PASSWORD")).orElse("%~B=r^E^f?\"0pRm(");

  private static final String DB_NAME =
      Optional.ofNullable(System.getenv("GCLOUD_CLOUDSQL_DB")).orElse("e2etest");
  private static final String NAMESPACE = "google-cloud-namespace";
  private static final int MOCKSERVER_PORT = 3000;
  private static final String LOCAL_IP = "127.0.0.1";
  private static final String CREDENTIALS_FILE = "build/gcloud/gcloud-service-key.json";

  private static SparkSession spark;
  private static ClientAndServer mockServer;

  private static String jdbcURL;

  private static GenericContainer<?> cloudSqlProxy;

  private static JdbcClient jdbcClient;

  @BeforeAll
  @SneakyThrows
  public static void beforeAll() {
    SparkSession$.MODULE$.cleanupAnyExistingSession();
    mockServer = new ClientAndServer(MOCKSERVER_PORT);
    MockServerUtils.configureStandardExpectation(mockServer);
    cloudSqlProxy =
        SparkContainerUtils.makeCloudSqlProxyContainer(
            Paths.get("/home/dominik/dev/openlineage/gcp-open-lineage-testing-6d2aa0caca8e.json"),
            getFullInstanceId());
    cloudSqlProxy.start();
    Integer mappedPort = cloudSqlProxy.getFirstMappedPort();
    jdbcURL = String.format("jdbc:postgresql://127.0.0.1:%s/%s", mappedPort, DB_NAME);
    jdbcClient = JdbcClient.init(jdbcURL, DB_USER, DB_PASSWORD);
    jdbcClient.executeStatements(CloudSqlTestData.INIT_SQL);
  }

  @AfterAll
  @SneakyThrows
  public static void afterAll() {
    SparkSession$.MODULE$.cleanupAnyExistingSession();
    MockServerUtils.stopMockServer(mockServer);
    jdbcClient.executeStatements(CloudSqlTestData.DROP_TABLES);
    cloudSqlProxy.stop();
    jdbcClient.close();
  }

  @BeforeEach
  @SneakyThrows
  public void beforeEach() {
    MockServerUtils.clearRequests(mockServer);
    spark =
        SparkSession.builder()
            .master("local[*]")
            .appName("GoogleCloudSqlIntegrationTest")
            .config("spark.driver.host", LOCAL_IP)
            .config("spark.driver.bindAddress", LOCAL_IP)
            .config("spark.ui.enabled", false)
            .config("spark.sql.shuffle.partitions", 1)
            .config("spark.extraListeners", OpenLineageSparkListener.class.getCanonicalName())
            .config("spark.openlineage.transport.type", "http")
            .config(
                "spark.openlineage.transport.url",
                "http://localhost:" + mockServer.getPort() + "/api/v1/lineage")
            .config("spark.openlineage.facets.disabled", "spark_unknown;spark.logicalPlan")
            .config("spark.openlineage.debugFacet", "disabled")
            .config("spark.openlineage.namespace", NAMESPACE)
            .config("parentProject", PROJECT_ID)
            .config("credentialsFile", CREDENTIALS_FILE)
            .getOrCreate();
  }

  @Test
  @SuppressWarnings("PMD.JUnitTestsShouldIncludeAssert")
  void columnLevelLineageSingleDestinationTest() {
    Dataset<Row> readDf =
        spark
            .read()
            .format("jdbc")
            .option("url", jdbcURL)
            .option("dbtable", "ol_clients")
            .option("user", DB_USER)
            .option("password", DB_PASSWORD)
            .option("driver", "org.postgresql.Driver")
            .load()
            .select("client_name", "client_category", "client_rating");

    readDf
        .write()
        .format("jdbc")
                .option("url", jdbcURL)
        .option("driver", "org.postgresql.Driver")
        .option("user", DB_USER)
        .option("password", DB_PASSWORD)
        .option("dbtable", "second_ol_clients")
        .mode("overwrite")
        .save();

    MockServerUtils.verifyEvents(mockServer, "cloudsqlSingleInputComplete.json");

    List<OpenLineage.RunEvent> events = MockServerUtils.getEventsEmitted(mockServer);
    OpenLineage.RunEvent lastEvent = events.get(events.size() - 1);
    assertThat(lastEvent.getInputs().get(0).getNamespace()).startsWith("postgres://127.0.0.1:");
    assertThat(lastEvent.getOutputs().get(0).getNamespace()).startsWith("postgres://127.0.0.1:");
  }

  @Test
  void columnLevelLineageTest() {
    final String jdbcQuery =
        "select js1.k, CONCAT(js1.j1, js2.j2) as j from jdbc_source1 js1 join jdbc_source2 js2 on js1.k = js2.k";

    Dataset<Row> df1 =
        spark
            .read()
            .format("jdbc")
            .option("url", jdbcURL)
            .option("query", jdbcQuery)
            .option("user", DB_USER)
            .option("password", DB_PASSWORD)
            .option("driver", "org.postgresql.Driver")
            .load();

    df1.registerTempTable("jdbc_result");

    final String query = "select j as value from jdbc_result j";

    spark
        .sql(query)
        .write()
        .format("jdbc")
        .option("url", jdbcURL)
        .option("dbtable", "test")
        .option("user", DB_USER)
        .option("password", DB_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .mode(SaveMode.Append)
        .save();

    MockServerUtils.verifyEvents(mockServer, "cloudsqlJDBCStart.json", "cloudsqlJDBCComplete.json");
  }

  private static String getFullInstanceId() {
    return String.format("%s:%s:%s", PROJECT_ID, REGION, INSTANCE_NAME);
  }

  private static class CloudSqlTestData {
    public static final List<String> DROP_TABLES =
        readAndParseStatements("/cloudsql/drop_tables_pg.sql");
    public static final List<String> INIT_SQL = readAndParseStatements("/column_lineage/init.sql");

    private static List<String> readAndParseStatements(String path) {
      String fileContent = readFileAsString(path);
      return Arrays.stream(fileContent.split(";"))
          .map(String::trim)
          .filter(s -> !s.isEmpty() && !"\n".equals(s))
          .collect(Collectors.toList());
    }

    private static String readFileAsString(String path) {
      try (InputStream is = CloudSqlTestData.class.getResourceAsStream(path)) {
        String fileContent = IOUtils.toString(is, StandardCharsets.UTF_8);
        return fileContent;
      } catch (IOException e) {
        throw new RuntimeException("Failed to read file");
      }
    }
  }

  private static class JdbcClient {
    private final HikariDataSource connectionPool;

    private JdbcClient(HikariDataSource connectionPool) {
      this.connectionPool = connectionPool;
    }

    public void executeStatements(List<String> statements) {
      try (Connection conn = connectionPool.getConnection()) {
        statements.forEach(
            s -> {
              try {
                PreparedStatement ps = conn.prepareStatement(s);
                ps.execute();
              } catch (SQLException e) {
                throw new RuntimeException("Failed to execute SQL: " + s, e);
              }
            });
      } catch (SQLException e) {
        throw new RuntimeException("Failed to get connection", e);
      }
    }

    public void close() {
      if (connectionPool.isRunning()) {
        connectionPool.close();
      }
    }

    public static JdbcClient init(String jdbcURL, String user, String password) {
      Properties connProps = new Properties();
      connProps.setProperty("user", user);
      connProps.setProperty("password", password);
      connProps.setProperty("sslmode", "disable");

      HikariConfig config = new HikariConfig();
      config.setJdbcUrl(jdbcURL);
      config.setDataSourceProperties(connProps);
      config.setConnectionTimeout(10000); // 10s
      HikariDataSource connectionPool = new HikariDataSource(config);
      return new JdbcClient(connectionPool);
    }
  }
}
