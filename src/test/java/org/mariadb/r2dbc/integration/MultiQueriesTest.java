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

import org.mariadb.r2dbc.BaseTest;
import org.mariadb.r2dbc.MariadbConnectionConfiguration;
import org.mariadb.r2dbc.MariadbConnectionFactory;
import org.mariadb.r2dbc.TestConfiguration;
import org.mariadb.r2dbc.api.MariadbConnection;
import io.r2dbc.spi.R2dbcBadGrammarException;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

import java.util.Optional;

public class MultiQueriesTest extends BaseTest {

  @Test
  void multiQueryDefault() {
    sharedConn
        .createStatement("SELECT 1; SELECT 'a'")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> row.get(0)))
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcBadGrammarException
                    && throwable.getMessage().contains("You have an error in your SQL syntax"))
        .verify();
  }

  @Test
  void multiQueryEnable() throws Exception {
    MariadbConnectionConfiguration conf =
        TestConfiguration.defaultBuilder.clone().allowMultiQueries(true).build();
    MariadbConnection connection = new MariadbConnectionFactory(conf).create().block();
    connection
        .createStatement("SELECT CAST(1 as CHAR); SELECT 'a'")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0))))
        .as(StepVerifier::create)
        .expectNext(Optional.of("1"), Optional.of("a"))
        .verifyComplete();
    connection.close().subscribe();
  }

  @Test
  void multiQueryDisable() throws Exception {
    MariadbConnectionConfiguration conf =
        TestConfiguration.defaultBuilder.clone().allowMultiQueries(false).build();
    MariadbConnection connection = new MariadbConnectionFactory(conf).create().block();
    connection
        .createStatement("SELECT CAST(1 as CHAR); SELECT 'a'")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> row.get(0)))
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcBadGrammarException
                    && throwable.getMessage().contains("You have an error in your SQL syntax"))
        .verify();
    connection.close().subscribe();
  }

  @Test
  void multiQueryWithParameterDefault() {
    sharedConn
        .createStatement("SELECT CAST(? as CHAR); SELECT ?")
        .bind(0, 1)
        .bind(1, "a")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> row.get(0)))
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcBadGrammarException
                    && throwable.getMessage().contains("You have an error in your SQL syntax"))
        .verify();
  }

  @Test
  void multiQueryWithParameterEnable() throws Exception {
    MariadbConnectionConfiguration conf =
        TestConfiguration.defaultBuilder.clone().allowMultiQueries(true).build();
    MariadbConnection connection = new MariadbConnectionFactory(conf).create().block();
    connection
        .createStatement("SELECT CAST(? as CHAR); SELECT ?")
        .bind(0, 1)
        .bind(1, "a")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0))))
        .as(StepVerifier::create)
        .expectNext(Optional.of("1"), Optional.of("a"))
        .verifyComplete();
    connection.close().subscribe();
  }

  @Test
  void multiQueryWithParameterDisable() throws Exception {
    MariadbConnectionConfiguration conf =
        TestConfiguration.defaultBuilder.clone().allowMultiQueries(false).build();
    MariadbConnection connection = new MariadbConnectionFactory(conf).create().block();
    connection
        .createStatement("SELECT CAST(? as CHAR); SELECT ?")
        .bind(0, 1)
        .bind(1, "a")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> row.get(0)))
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcBadGrammarException
                    && throwable.getMessage().contains("You have an error in your SQL syntax"))
        .verify();
    connection.close().subscribe();
  }
}
