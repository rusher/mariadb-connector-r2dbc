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

package io.r2dbc.mariadb.integration.parameter;

import io.r2dbc.mariadb.BaseTest;
import io.r2dbc.spi.R2dbcTransientResourceException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Optional;

public class FloatParameterTest extends BaseTest {
  @BeforeAll
  public static void before2() {
    sharedConn
        .createStatement("CREATE TEMPORARY TABLE FloatParam (t1 FLOAT, t2 FLOAT, t3 FLOAT)")
        .execute()
        .subscribe();
    // ensure having same kind of result for truncation
    sharedConn.createStatement("SET @@sql_mode = 'STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION'").execute().blockLast();
  }

  @BeforeEach
  public void beforeEach() {
    sharedConn.createStatement("TRUNCATE TABLE FloatParam").execute().blockLast();
  }

  @Test
  void nullValue() {
    sharedConn
        .createStatement("INSERT INTO FloatParam VALUES (?,?,?)")
        .bindNull(0, Float.class)
        .bindNull(1, Float.class)
        .bindNull(2, Float.class)
        .execute()
        .blockLast();
    validate(Optional.empty(), 0f, Optional.empty(), 0f, Optional.empty(), 0f);
  }

  @Test
  void bigIntValue() {
    sharedConn
        .createStatement("INSERT INTO FloatParam VALUES (?,?,?)")
        .bind(0, BigInteger.ONE)
        .bind(1, new BigInteger("9223372036854775807"))
        .bind(2, new BigInteger("-9"))
        .execute()
        .blockLast();
    validate(
        Optional.of(1f),
        0f,
        Optional.of(9223372036854775807f),
        4000000000000f,
        Optional.of(-9f),
        0f);
  }

  @Test
  void stringValue() {
    sharedConn
        .createStatement("INSERT INTO FloatParam VALUES (?,?,?)")
        .bind(0, "1")
        .bind(1, "9223372036854775807")
        .bind(2, "-9")
        .execute()
        .blockLast();
    validate(
        Optional.of(1f),
        0f,
        Optional.of(9223372036854775807f),
        4000000000000f,
        Optional.of(-9f),
        0f);
  }

  @Test
  void decimalValue() {
    sharedConn
        .createStatement("INSERT INTO FloatParam VALUES (?,?,?)")
        .bind(0, BigDecimal.ONE)
        .bind(1, new BigDecimal("9223372036854775807"))
        .bind(2, new BigDecimal("-9"))
        .execute()
        .blockLast();

    validate(
        Optional.of(1f),
        0f,
        Optional.of(9223372036854775807f),
        4000000000000f,
        Optional.of(-9f),
        0f);
  }

  @Test
  void intValue() {
    sharedConn
        .createStatement("INSERT INTO FloatParam VALUES (?,?,?)")
        .bind(0, 1)
        .bind(1, -1)
        .bind(2, 0)
        .execute()
        .blockLast();
    validate(Optional.of(1f), 0f, Optional.of(-1f), 0f, Optional.of(0f), 0f);
  }

  @Test
  void byteValue() {
    sharedConn
        .createStatement("INSERT INTO FloatParam VALUES (?,?,?)")
        .bind(0, (byte) 127)
        .bind(1, (byte) -128)
        .bind(2, (byte) 0)
        .execute()
        .blockLast();
    validate(Optional.of(127f), 0f, Optional.of(-128f), 0f, Optional.of(0f), 0f);
  }

  @Test
  void floatValue() {
    sharedConn
        .createStatement("INSERT INTO FloatParam VALUES (?,?,?)")
        .bind(0, 127f)
        .bind(1, -128f)
        .bind(2, 0f)
        .execute()
        .blockLast();
    validate(Optional.of(127f), 0f, Optional.of(-128f), 0f, Optional.of(0f), 0f);
  }

  @Test
  void doubleValue() {
    sharedConn
        .createStatement("INSERT INTO FloatParam VALUES (?,?,?)")
        .bind(0, 127d)
        .bind(1, -128d)
        .bind(2, 0d)
        .execute()
        .blockLast();
    validate(Optional.of(127f), 0f, Optional.of(-128f), 0f, Optional.of(0f), 0f);
  }

  @Test
  void shortValue() {
    sharedConn
        .createStatement("INSERT INTO FloatParam VALUES (?,?,?)")
        .bind(0, Short.valueOf("1"))
        .bind(1, Short.valueOf("-1"))
        .bind(2, Short.valueOf("0"))
        .execute()
        .blockLast();
    validate(Optional.of(1f), 0f, Optional.of(-1f), 0f, Optional.of(0f), 0f);
  }

  @Test
  void longValue() {
    sharedConn
        .createStatement("INSERT INTO FloatParam VALUES (?,?,?)")
        .bind(0, Long.valueOf("1"))
        .bind(1, Long.valueOf("-1"))
        .bind(2, Long.valueOf("0"))
        .execute()
        .blockLast();
    validate(Optional.of(1f), 0f, Optional.of(-1f), 0f, Optional.of(0f), 0f);
  }

  @Test
  void localDateTimeValue() {
    sharedConn
        .createStatement("INSERT INTO FloatParam VALUES (?,?,?)")
        .bind(0, LocalDateTime.now())
        .bind(1, LocalDateTime.now())
        .bind(2, LocalDateTime.now())
        .execute()
        .flatMap(r -> r.getRowsUpdated())
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcTransientResourceException
                    && ((R2dbcTransientResourceException) throwable).getSqlState().equals("01000"))
        .verify();
  }

  @Test
  void localDateValue() {
    sharedConn
        .createStatement("INSERT INTO FloatParam VALUES (?,?,?)")
        .bind(0, LocalDate.now())
        .bind(1, LocalDate.now())
        .bind(2, LocalDate.now())
        .execute()
        .flatMap(r -> r.getRowsUpdated())
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcTransientResourceException
                    && ((R2dbcTransientResourceException) throwable).getSqlState().equals("01000"))
        .verify();
  }

  @Test
  void localTimeValue() {
    sharedConn
        .createStatement("INSERT INTO FloatParam VALUES (?,?,?)")
        .bind(0, LocalTime.now())
        .bind(1, LocalTime.now())
        .bind(2, LocalTime.now())
        .execute()
        .flatMap(r -> r.getRowsUpdated())
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcTransientResourceException
                    && ((R2dbcTransientResourceException) throwable).getSqlState().equals("01000"))
        .verify();
  }

  private void validate(
      Optional<Float> t1,
      float t1Delta,
      Optional<Float> t2,
      float t2Delta,
      Optional<Float> t3,
      float t3Delta) {
    sharedConn
        .createStatement("SELECT * FROM FloatParam")
        .execute()
        .flatMap(
            r ->
                r.map(
                    (row, metadata) ->
                        Flux.just(
                            Optional.ofNullable((Float) row.get(0)),
                            Optional.ofNullable((Float) row.get(1)),
                            Optional.ofNullable((Float) row.get(2)))))
        .blockLast()
        .as(StepVerifier::create)
        .expectNextMatches(
            val -> {
              if (!val.isPresent()) {
                return !t1.isPresent();
              }
              Assertions.assertEquals(val.get(), t1.get(), t1Delta);
              return true;
            })
        .expectNextMatches(
            val -> {
              if (!val.isPresent()) {
                return !t2.isPresent();
              }
              Assertions.assertEquals(val.get(), t2.get(), t2Delta);
              return true;
            })
        .expectNextMatches(
            val -> {
              if (!val.isPresent()) {
                return !t3.isPresent();
              }
              Assertions.assertEquals(val.get(), t3.get(), t3Delta);
              return true;
            })
        .verifyComplete();
  }
}
