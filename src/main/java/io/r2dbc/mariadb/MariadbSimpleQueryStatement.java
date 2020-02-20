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

import io.r2dbc.mariadb.api.MariadbStatement;
import io.r2dbc.mariadb.client.Client;
import io.r2dbc.mariadb.message.client.QueryPacket;
import io.r2dbc.mariadb.message.server.ServerMessage;
import io.r2dbc.mariadb.util.Assert;
import io.r2dbc.mariadb.util.ClientPrepareResult;
import reactor.core.publisher.Flux;
import reactor.util.annotation.Nullable;

final class MariadbSimpleQueryStatement implements MariadbStatement {

  private final Client client;
  private final String sql;
  private String[] generatedColumns;

  MariadbSimpleQueryStatement(Client client, String sql) {
    this.client = client;
    this.sql = Assert.requireNonNull(sql, "sql must not be null");
  }

  static boolean supports(String sql, Client client) {
    Assert.requireNonNull(sql, "sql must not be null");
    if (sql.contains("?") || sql.contains(":")) {
      return !ClientPrepareResult.hasParameter(sql, client.noBackslashEscapes());
    }
    return true;
  }

  @Override
  public MariadbSimpleQueryStatement add() {
    return this;
  }

  @Override
  public MariadbSimpleQueryStatement bind(@Nullable String identifier, @Nullable Object value) {
    throw new UnsupportedOperationException(
        String.format("Binding parameters is not supported for the statement '%s'", this.sql));
  }

  @Override
  public MariadbSimpleQueryStatement bind(int index, @Nullable Object value) {
    throw new UnsupportedOperationException(
        String.format("Binding parameters is not supported for the statement '%s'", this.sql));
  }

  @Override
  public MariadbSimpleQueryStatement bindNull(
      @Nullable String identifier, @Nullable Class<?> type) {
    throw new UnsupportedOperationException(
        String.format("Binding parameters is not supported for the statement '%s'", this.sql));
  }

  @Override
  public MariadbSimpleQueryStatement bindNull(int index, @Nullable Class<?> type) {
    throw new UnsupportedOperationException(
        String.format("Binding parameters is not supported for the statement '%s'", this.sql));
  }

  @Override
  public Flux<io.r2dbc.mariadb.api.MariadbResult> execute() {
    return execute(this.sql, this.generatedColumns);
  }

  @Override
  public MariadbSimpleQueryStatement fetchSize(int rows) {
    return this;
  }

  @Override
  public MariadbSimpleQueryStatement returnGeneratedValues(String... columns) {
    Assert.requireNonNull(columns, "columns must not be null");

    if (!(client.getVersion().isMariaDBServer()
        && client.getVersion().versionGreaterOrEqual(10, 5, 1))) {
      throw new IllegalStateException(
          "Server does not support RETURNING clause (require MariaDB 10.5.1 version)");
    }

    ClientPrepareResult prepareResult =
        ClientPrepareResult.parameterParts(this.sql, this.client.noBackslashEscapes());
    prepareResult.validateAddingReturning();

    this.generatedColumns = columns;
    return this;
  }

  @Override
  public String toString() {
    return "MariadbSimpleQueryStatement{"
        + "client="
        + this.client
        + ", sql='"
        + this.sql
        + '\''
        + '}';
  }

  private Flux<io.r2dbc.mariadb.api.MariadbResult> execute(String sql, String[] generatedColumns) {
    ExceptionFactory factory = ExceptionFactory.withSql(sql);

    if (generatedColumns != null) {
      sql =
          String.format(
              "%s RETURNING %s",
              sql, generatedColumns.length == 0 ? "*" : String.join(", ", generatedColumns));
    }

    Flux<ServerMessage> response = this.client.sendCommand(new QueryPacket(sql));
    return response
        .windowUntil(it -> it.resultSetEnd())
        .map(dataRow -> new MariadbResult(true, dataRow, factory));
  }
}
