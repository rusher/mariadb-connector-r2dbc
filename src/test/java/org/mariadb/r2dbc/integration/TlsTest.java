/*
 * Copyright 2020 MariaDB Ab.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.mariadb.r2dbc.integration;

import org.mariadb.r2dbc.*;
import org.mariadb.r2dbc.api.MariadbConnection;
import org.mariadb.r2dbc.api.MariadbConnectionMetadata;
import io.r2dbc.spi.R2dbcNonTransientException;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import javax.net.ssl.SSLHandshakeException;
import java.io.File;
import java.util.Arrays;

public class TlsTest extends BaseTest {

  public static String serverSslCert;
  public static String clientSslCert;
  public static String clientSslKey;

  @BeforeAll
  public static void before2() {
    serverSslCert = System.getenv("TEST_SERVER_SSL_CERT");
    clientSslCert = System.getenv("TEST_CLIENT_SSL_CERT");
    clientSslKey = System.getenv("TEST_CLIENT_KEY");

    // try default if not present
    if (serverSslCert == null) {
      File sslDir = new File(System.getProperty("user.dir") + "/../ssl");
      if (sslDir.exists() && sslDir.isDirectory()) {

        serverSslCert = System.getProperty("user.dir") + "/../ssl/server.crt";
        clientSslCert = System.getProperty("user.dir") + "/../ssl/client.crt";
        clientSslKey = System.getProperty("user.dir") + "/../ssl/client.key";
      }
    }

    boolean useOldNotation = true;
    MariadbConnectionMetadata meta = sharedConn.getMetadata();
    if ((meta.isMariaDBServer() && meta.minVersion(10, 2, 0))
        || (!meta.isMariaDBServer() && meta.minVersion(8, 0, 0))) {
      useOldNotation = false;
    }

    sharedConn
        .createStatement("DROP USER 'MUTUAL_AUTH'")
        .execute()
        .map(res -> res.getRowsUpdated())
        .onErrorReturn(Mono.empty())
        .subscribe();
    String create_sql;
    String grant_sql;
    if (useOldNotation) {
      create_sql = "CREATE USER 'MUTUAL_AUTH'";
      grant_sql =
          "grant all privileges on *.* to 'MUTUAL_AUTH' identified by 'ssltestpassword' REQUIRE X509";
    } else {
      create_sql = "CREATE USER 'MUTUAL_AUTH' identified by 'ssltestpassword' REQUIRE X509";
      grant_sql = "grant all privileges on *.* to 'MUTUAL_AUTH'";
    }
    sharedConn.createStatement(create_sql).execute().subscribe();
    sharedConn.createStatement(grant_sql).execute().subscribe();
    sharedConn.createStatement("FLUSH PRIVILEGES").execute().blockLast();
  }

  @Test
  void defaultHasNoSSL() throws Exception {
    Assumptions.assumeTrue(haveSsl(sharedConn));
    sharedConn
        .createStatement("SHOW STATUS like 'Ssl_version'")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> row.get(1)))
        .as(StepVerifier::create)
        .expectNextMatches(
            val -> {
              String[] values = {"TLSv1", "TLSv1.1", "TLSv1.2", "TLSv1.3"};
              return !Arrays.stream(values).anyMatch(val::equals);
            })
        .verifyComplete();
  }

  @Test
  void trustValidation() throws Exception {
    Assumptions.assumeTrue(haveSsl(sharedConn));
    MariadbConnectionConfiguration conf =
        TestConfiguration.defaultBuilder.clone().sslMode(SslMode.ENABLE_TRUST).build();
    MariadbConnection connection = new MariadbConnectionFactory(conf).create().block();
    connection
        .createStatement("SHOW STATUS like 'Ssl_version'")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> row.get(1)))
        .as(StepVerifier::create)
        .expectNextMatches(
            val -> {
              String[] values = {"TLSv1", "TLSv1.1", "TLSv1.2", "TLSv1.3"};
              return Arrays.stream(values).anyMatch(val::equals);
            })
        .verifyComplete();
    connection.close().block();
  }

  @Test
  void trustForceProtocol() throws Exception {
    Assumptions.assumeTrue(haveSsl(sharedConn));
    MariadbConnectionConfiguration conf =
        TestConfiguration.defaultBuilder
            .clone()
            .sslMode(SslMode.ENABLE_TRUST)
            .tlsProtocol("TLSv1.1")
            .build();
    MariadbConnection connection = new MariadbConnectionFactory(conf).create().block();
    connection
        .createStatement("SHOW STATUS like 'Ssl_version'")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> row.get(1)))
        .as(StepVerifier::create)
        .expectNext("TLSv1.1")
        .verifyComplete();
    connection.close().block();
  }

  @Test
  void withoutHostnameValidation() throws Exception {
    Assumptions.assumeTrue(haveSsl(sharedConn));
    Assumptions.assumeTrue(serverSslCert != null);
    MariadbConnectionConfiguration conf =
        TestConfiguration.defaultBuilder
            .clone()
            .sslMode(SslMode.ENABLE_WITHOUT_HOSTNAME_VERIFICATION)
            .serverSslCert(serverSslCert)
            .build();
    MariadbConnection connection = new MariadbConnectionFactory(conf).create().block();
    connection
        .createStatement("SHOW STATUS like 'Ssl_version'")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> row.get(1)))
        .as(StepVerifier::create)
        .expectNextMatches(
            val -> {
              String[] values = {"TLSv1", "TLSv1.1", "TLSv1.2", "TLSv1.3"};
              return Arrays.stream(values).anyMatch(val::equals);
            })
        .verifyComplete();
    connection.close().block();
  }

  @Test
  void fullWithoutServerCert() throws Exception {
    Assumptions.assumeTrue(haveSsl(sharedConn));
    MariadbConnectionConfiguration conf =
        TestConfiguration.defaultBuilder.clone().sslMode(SslMode.ENABLE).build();
    new MariadbConnectionFactory(conf)
        .create()
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcNonTransientException
                    && (throwable.getCause().getCause() instanceof SSLHandshakeException
                        || throwable.getCause() instanceof SSLHandshakeException))
        .verify();
  }

  @Test
  void fullValidationFailing() throws Exception {
    Assumptions.assumeTrue(haveSsl(sharedConn));
    Assumptions.assumeTrue(serverSslCert != null);
    MariadbConnectionConfiguration conf =
        TestConfiguration.defaultBuilder
            .clone()
            .sslMode(SslMode.ENABLE)
            .serverSslCert(serverSslCert)
            .build();
    if (!conf.getHost().equals("mariadb.example.com")) {
      new MariadbConnectionFactory(conf)
              .create()
              .as(StepVerifier::create)
              .expectErrorMatches(
                      throwable ->
                              throwable instanceof R2dbcNonTransientException
                                      && throwable.getMessage().contains("SSL hostname verification failed "))
              .verify();
    } else {
      MariadbConnection connection = new MariadbConnectionFactory(conf).create().block();
      connection
              .createStatement("SHOW STATUS like 'Ssl_version'")
              .execute()
              .flatMap(r -> r.map((row, metadata) -> row.get(1)))
              .as(StepVerifier::create)
              .expectNextMatches(
                      val -> {
                        String[] values = {"TLSv1", "TLSv1.1", "TLSv1.2", "TLSv1.3"};
                        return Arrays.stream(values).anyMatch(val::equals);
                      })
              .verifyComplete();
      connection.close().block();
    }
  }

  @Test
  void fullValidation() throws Exception {
    Assumptions.assumeTrue(haveSsl(sharedConn));
    Assumptions.assumeTrue(serverSslCert != null);
    MariadbConnectionConfiguration conf =
        TestConfiguration.defaultBuilder
            .clone()
            .sslMode(SslMode.ENABLE)
            .host("mariadb.example.com")
            .serverSslCert(serverSslCert)
            .build();
    MariadbConnection connection = new MariadbConnectionFactory(conf).create().block();
    connection
        .createStatement("SHOW STATUS like 'Ssl_version'")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> row.get(1)))
        .as(StepVerifier::create)
        .expectNextMatches(
            val -> {
              String[] values = {"TLSv1", "TLSv1.1", "TLSv1.2", "TLSv1.3"};
              return Arrays.stream(values).anyMatch(val::equals);
            })
        .verifyComplete();
    connection.close().block();
  }

  @Test
  void fullMutualWithoutClientCerts() throws Exception {
    Assumptions.assumeTrue(haveSsl(sharedConn));
    Assumptions.assumeTrue(serverSslCert != null && clientSslCert != null & clientSslKey != null);
    MariadbConnectionConfiguration conf =
        TestConfiguration.defaultBuilder
            .clone()
            .sslMode(SslMode.ENABLE)
            .username("MUTUAL_AUTH")
            .password("ssltestpassword")
            .host("mariadb.example.com")
            .serverSslCert(serverSslCert)
            .clientSslKey(clientSslKey)
            .build();
    new MariadbConnectionFactory(conf)
        .create()
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcNonTransientException
                    && throwable.getMessage().contains("Access denied"))
        .verify();
  }

  @Test
  void fullMutualAuthentication() throws Exception {
    Assumptions.assumeTrue(haveSsl(sharedConn));
    Assumptions.assumeTrue(serverSslCert != null && clientSslCert != null & clientSslKey != null);
    MariadbConnectionConfiguration conf =
        TestConfiguration.defaultBuilder
            .clone()
            .sslMode(SslMode.ENABLE)
            .username("MUTUAL_AUTH")
            .password("ssltestpassword")
            .host("mariadb.example.com")
            .serverSslCert(serverSslCert)
            .clientSslCert(clientSslCert)
            .clientSslKey(clientSslKey)
            .build();
    MariadbConnection connection = new MariadbConnectionFactory(conf).create().block();
    connection
        .createStatement("SHOW STATUS like 'Ssl_version'")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> row.get(1)))
        .as(StepVerifier::create)
        .expectNextMatches(
            val -> {
              String[] values = {"TLSv1", "TLSv1.1", "TLSv1.2", "TLSv1.3"};
              return Arrays.stream(values).anyMatch(val::equals);
            })
        .verifyComplete();
    connection.close().block();
  }
}
