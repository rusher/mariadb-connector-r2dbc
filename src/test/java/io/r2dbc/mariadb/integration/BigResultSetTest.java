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
import io.r2dbc.mariadb.api.MariadbConnection;
import io.r2dbc.mariadb.api.MariadbConnectionMetadata;
import io.r2dbc.mariadb.api.MariadbResult;
import io.r2dbc.spi.Connection;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.atomic.AtomicInteger;

public class BigResultSetTest extends BaseTest {

  @Test
  void BigResultSet() {
    MariadbConnectionMetadata meta = sharedConn.getMetadata();
    // sequence table requirement
    Assumptions.assumeTrue(meta.isMariaDBServer() && minVersion(10,1,0));

    sharedConn
        .createStatement(
            "SELECT lpad(conv(floor(rand()*pow(36,8)), 10, 36), 8, 0) as rnd_str_8 FROM seq_1_to_10000")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> row.get(0)))
        .as(StepVerifier::create)
        .expectNextCount(10000)
        .verifyComplete();
  }

  @Test
  void multipleFluxSubscription() {
    MariadbConnectionMetadata meta = sharedConn.getMetadata();
    // sequence table requirement
    Assumptions.assumeTrue(meta.isMariaDBServer() && minVersion(10,1,0));
    Flux<MariadbResult> res = sharedConn.createStatement("SELECT * FROM seq_1_to_50").execute();

    Flux<String> flux1 =
        res.flatMap(r -> r.map((row, metadata) -> row.get(0, String.class))).share();

    AtomicInteger total = new AtomicInteger();
    for (int i = 0; i < 10; i++) {
      flux1.subscribe(out -> total.incrementAndGet());
    }

    flux1.doOnComplete(() -> Assertions.assertEquals(500, total.get())).blockLast();
  }


  @Test
  void multiPacketRow() {
    Assumptions.assumeTrue(checkMaxAllowedPacketMore20m(sharedConn));
    final char[] array19m = new char[19000000];
    for (int i = 0; i < array19m.length; i++) {
      array19m[i] = (char) (0x30 + (i % 10));
    }

    sharedConn.createStatement("CREATE TEMPORARY TABLE multiPacketRow(val LONGTEXT)").execute().subscribe();
    sharedConn.createStatement("INSERT INTO multiPacketRow VALUES (?)")
        .bind(0, new String(array19m))
        .execute()
        .blockLast();
    Assertions.assertArrayEquals(array19m, sharedConn.createStatement("SELECT * FROM multiPacketRow")
            .execute()
            .flatMap(r -> r.map((row, metadata) -> row.get(0, String.class))).blockLast().toCharArray());
  }

  public boolean checkMaxAllowedPacketMore20m(MariadbConnection connection) {
    long maxAllowedPacket = connection.createStatement("select @@max_allowed_packet")
            .execute()
            .flatMap(r -> r.map((row, metadata) -> row.get(0, Long.class))).blockLast();
    return maxAllowedPacket >= 20 * 1024 * 1024L;
  }
}
