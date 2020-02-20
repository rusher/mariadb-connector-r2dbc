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

package io.r2dbc.mariadb.integration;

import io.r2dbc.mariadb.BaseTest;
import io.r2dbc.mariadb.MariadbConnectionConfiguration;
import io.r2dbc.mariadb.MariadbConnectionFactory;
import io.r2dbc.mariadb.TestConfiguration;
import io.r2dbc.mariadb.api.MariadbConnection;
import io.r2dbc.mariadb.api.MariadbStatement;
import io.r2dbc.spi.R2dbcNonTransientResourceException;
import io.r2dbc.spi.ValidationDepth;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.math.BigInteger;
import java.time.Duration;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class ConnectionTest extends BaseTest {

  @Test
  void localValidation() {
    sharedConn
        .validate(ValidationDepth.LOCAL)
        .as(StepVerifier::create)
        .expectNext(Boolean.TRUE)
        .verifyComplete();
  }

  @Test
  void localValidationClosedConnection() {
    MariadbConnection connection = factory.create().block();
    connection.close().subscribe();
    connection
        .validate(ValidationDepth.LOCAL)
        .as(StepVerifier::create)
        .expectNext(Boolean.FALSE)
        .verifyComplete();
  }

  @Test
  void remoteValidation() {
    sharedConn
        .validate(ValidationDepth.REMOTE)
        .as(StepVerifier::create)
        .expectNext(Boolean.TRUE)
        .verifyComplete();
  }

  @Test
  void remoteValidationClosedConnection() {
    MariadbConnection connection = factory.create().block();
    connection.close().subscribe();
    connection
        .validate(ValidationDepth.REMOTE)
        .as(StepVerifier::create)
        .expectNext(Boolean.FALSE)
        .verifyComplete();
  }

  @Test
  void multipleConnection() {
    for (int i = 0; i < 50; i++) {
      MariadbConnection connection = factory.create().block();
      connection
          .validate(ValidationDepth.REMOTE)
          .as(StepVerifier::create)
          .expectNext(Boolean.TRUE)
          .verifyComplete();
      connection.close().subscribe();
    }
  }

  @Test
  void connectTimeout() throws Exception {
    MariadbConnectionConfiguration conf =
        TestConfiguration.defaultBuilder.clone().connectTimeout(Duration.ofSeconds(1)).build();
    MariadbConnection connection = new MariadbConnectionFactory(conf).create().block();
    consume(connection);
    connection.close();
  }

  @Test
  void multipleClose() throws Exception {
    MariadbConnection connection = factory.create().block();
    connection.close().subscribe();
    connection.close().block();
  }

  @Test
  void multipleBegin() throws Exception {
    MariadbConnection connection = factory.create().block();
    connection.beginTransaction().subscribe();
    connection.beginTransaction().block();
    connection.beginTransaction().block();
    connection.close().block();
  }

  @Test
  void multipleAutocommit() throws Exception {
    MariadbConnection connection = factory.create().block();
    connection.setAutoCommit(true).subscribe();
    connection.setAutoCommit(true).block();
    connection.setAutoCommit(false).block();
    connection.close().block();
  }

  @Test
  void queryAfterClose() throws Exception {
    MariadbConnection connection = factory.create().block();
    MariadbStatement stmt = connection.createStatement("SELECT 1");
    connection.close().block();
    stmt.execute()
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcNonTransientResourceException
                    && throwable.getMessage().contains("Connection is close. Cannot send anything"))
        .verify();
  }

  private void consume(io.r2dbc.spi.Connection connection) {
    int loop = 100;
    int numberOfUserCol = 41;
    io.r2dbc.spi.Statement statement =
        connection.createStatement("select * FROM mysql.user LIMIT 1");

    Flux<Object[]> lastOne;
    lastOne = stat(statement, numberOfUserCol);
    while (loop-- > 0) {
      lastOne = lastOne.thenMany(stat(statement, numberOfUserCol));
    }
    Object[] obj = lastOne.blockLast();
  }

  private Flux<Object[]> stat(io.r2dbc.spi.Statement statement, int numberOfUserCol) {
    return Flux.from(statement.execute())
        .flatMap(
            it ->
                it.map(
                    (row, rowMetadata) -> {
                      Object[] objs = new Object[numberOfUserCol];
                      for (int i = 0; i < numberOfUserCol; i++) {
                        objs[i] = row.get(i);
                      }
                      return objs;
                    }));
  }

  @Test
  void multiThreading() throws Throwable {
    AtomicInteger completed = new AtomicInteger(0);
    ThreadPoolExecutor scheduler =
        new ThreadPoolExecutor(10, 20, 50, TimeUnit.SECONDS, new LinkedBlockingQueue<>());

    for (int i = 0; i < 100; i++) {
      scheduler.execute(new ExecuteQueries(completed));
    }
    scheduler.shutdown();
    scheduler.awaitTermination(120, TimeUnit.SECONDS);
    Assertions.assertEquals(100, completed.get());
  }

  @Test
  void multiThreadingSameConnection() throws Throwable {
    AtomicInteger completed = new AtomicInteger(0);
    ThreadPoolExecutor scheduler =
        new ThreadPoolExecutor(10, 20, 50, TimeUnit.SECONDS, new LinkedBlockingQueue<>());

    MariadbConnection connection = factory.create().block();

    for (int i = 0; i < 100; i++) {
      scheduler.execute(new ExecuteQueriesOnSameConnection(completed, connection));
    }
    scheduler.shutdown();
    scheduler.awaitTermination(120, TimeUnit.SECONDS);
    connection.close().subscribe();
    Assertions.assertEquals(100, completed.get());
  }

  protected class ExecuteQueries implements Runnable {
    private AtomicInteger i;

    public ExecuteQueries(AtomicInteger i) {
      this.i = i;
    }

    public void run() {
      MariadbConnection connection = null;
      try {
        connection = factory.create().block();
        int rnd = (int) (Math.random() * 1000);
        io.r2dbc.spi.Statement statement = connection.createStatement("select " + rnd);
        BigInteger val =
            Flux.from(statement.execute())
                .flatMap(it -> it.map((row, rowMetadata) -> row.get(0, BigInteger.class)))
                .blockLast();
        if (rnd != val.intValue())
          throw new IllegalStateException("ERROR rnd:" + rnd + " different to val:" + val);
        i.incrementAndGet();
      } catch (Throwable e) {
        e.printStackTrace();
      } finally {
        if (connection != null) connection.close().subscribe();
      }
    }
  }

  protected class ExecuteQueriesOnSameConnection implements Runnable {
    private AtomicInteger i;
    private MariadbConnection connection;

    public ExecuteQueriesOnSameConnection(AtomicInteger i, MariadbConnection connection) {
      this.i = i;
      this.connection = connection;
    }

    public void run() {
      try {
        int rnd = (int) (Math.random() * 1000);
        io.r2dbc.spi.Statement statement = connection.createStatement("select " + rnd);
        BigInteger val =
            Flux.from(statement.execute())
                .flatMap(it -> it.map((row, rowMetadata) -> row.get(0, BigInteger.class)))
                .blockFirst();
        if (rnd != val.intValue())
          throw new IllegalStateException("ERROR rnd:" + rnd + " different to val:" + val);
        i.incrementAndGet();
      } catch (Throwable e) {
        e.printStackTrace();
      }
    }
  }
}
