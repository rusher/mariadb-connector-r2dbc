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

package io.r2dbc.mariadb;

import io.r2dbc.mariadb.message.server.ErrorPacket;
import io.r2dbc.mariadb.message.server.ServerMessage;
import io.r2dbc.spi.*;
import reactor.core.publisher.SynchronousSink;

public final class ExceptionFactory {

  public static final ExceptionFactory INSTANCE = new ExceptionFactory("");
  private final String sql;

  private ExceptionFactory(String sql) {
    this.sql = sql;
  }

  public static ExceptionFactory withSql(String sql) {
    return new ExceptionFactory(sql);
  }

  public static R2dbcException createException(ErrorPacket error, String sql) {

    if ("70100".equals(error.getSqlState())) { // ER_QUERY_INTERRUPTED
      return new R2dbcTimeoutException(
          error.getMessage(), error.getSqlState(), error.getErrorCode());
    }

    String sqlClass = error.getSqlState().substring(0, 2);
    switch (sqlClass) {
      case "0A":
      case "22":
      case "26":
      case "2F":
      case "20":
      case "42":
      case "XA":
        return new R2dbcBadGrammarException(
            error.getMessage(), error.getSqlState(), error.getErrorCode(), sql);
      case "25":
      case "28":
        return new R2dbcPermissionDeniedException(
            error.getMessage(), error.getSqlState(), error.getErrorCode());
      case "21":
      case "23":
        return new R2dbcDataIntegrityViolationException(
            error.getMessage(), error.getSqlState(), error.getErrorCode());
      case "08":
        return new R2dbcNonTransientResourceException(
            error.getMessage(), error.getSqlState(), error.getErrorCode());
      case "40":
        return new R2dbcRollbackException(
            error.getMessage(), error.getSqlState(), error.getErrorCode());
    }

    return new R2dbcTransientResourceException(
        error.getMessage(), error.getSqlState(), error.getErrorCode());
  }

  public R2dbcException from(ErrorPacket err) {
    return createException(err, this.sql);
  }

  public void handleErrorResponse(ServerMessage message, SynchronousSink<ServerMessage> sink) {
    if (message instanceof ErrorPacket) {
      sink.error(createException((ErrorPacket) message, this.sql));
    } else {
      sink.next(message);
    }
  }

  @Override
  public String toString() {
    return "ExceptionFactory{" + "sql='" + sql + '\'' + '}';
  }
}
