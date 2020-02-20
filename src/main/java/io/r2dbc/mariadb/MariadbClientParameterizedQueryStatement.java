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
import io.r2dbc.mariadb.codec.Codec;
import io.r2dbc.mariadb.codec.Codecs;
import io.r2dbc.mariadb.codec.Parameter;
import io.r2dbc.mariadb.message.client.QueryWithParametersPacket;
import io.r2dbc.mariadb.message.server.ServerMessage;
import io.r2dbc.mariadb.util.Assert;
import io.r2dbc.mariadb.util.ClientPrepareResult;
import reactor.core.publisher.Flux;
import reactor.util.annotation.Nullable;

import java.util.Arrays;

final class MariadbClientParameterizedQueryStatement implements MariadbStatement {

  private final Client client;
  private final String sql;
  private final ClientPrepareResult prepareResult;
  private final Parameter<?>[] parameters;
  private final MariadbConnectionConfiguration configuration;
  private String[] generatedColumns;

  MariadbClientParameterizedQueryStatement(
      Client client, String sql, MariadbConnectionConfiguration configuration) {
    this.client = client;
    this.configuration = configuration;
    this.sql = Assert.requireNonNull(sql, "sql must not be null");
    this.prepareResult =
        ClientPrepareResult.parameterParts(this.sql, this.client.noBackslashEscapes());
    this.parameters = new Parameter<?>[prepareResult.getParamCount()];
  }

  static boolean supports(String sql) {
    Assert.requireNonNull(sql, "sql must not be null");
    return !sql.trim().isEmpty();
  }

  @Override
  public MariadbClientParameterizedQueryStatement add() {
    // check valid parameters
    for (int i = 0; i < prepareResult.getParamCount(); i++) {
      if (parameters[i] == null) {
        throw new IllegalArgumentException(String.format("Parameter at position %i is not set", i));
      }
    }

    return new MariadbClientParameterizedQueryStatement(client, sql, configuration);
  }

  @Override
  public MariadbClientParameterizedQueryStatement bind(
      @Nullable String identifier, @Nullable Object value) {
    Assert.requireNonNull(identifier, "identifier cannot be null");
    return bind(getColumn(identifier), value);
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  @Override
  public MariadbClientParameterizedQueryStatement bind(int index, @Nullable Object value) {
    if (index >= prepareResult.getParamCount() || index < 0) {
      throw new IndexOutOfBoundsException(
          String.format(
              "index must be in 0-%d range but value is %d",
              prepareResult.getParamCount() - 1, index));
    }
    if (value == null) return bindNull(index, null);

    for (Codec<?> codec : Codecs.LIST) {
      if (codec.canEncode(value)) {
        parameters[index] = (Parameter<?>) new Parameter(codec, value);
        return this;
      }
    }
    throw new IllegalArgumentException(
        String.format(
            "No encoder for class %s (parameter at index %s) ", value.getClass().getName(), index));
  }

  @Override
  public MariadbClientParameterizedQueryStatement bindNull(
      @Nullable String identifier, @Nullable Class<?> type) {
    Assert.requireNonNull(identifier, "identifier cannot be null");
    return bindNull(getColumn(identifier), type);
  }

  @Override
  public MariadbClientParameterizedQueryStatement bindNull(int index, @Nullable Class<?> type) {
    if (index >= prepareResult.getParamCount() || index < 0) {
      throw new IndexOutOfBoundsException(
          String.format(
              "index must be in 0-%d range but value is " + "%d",
              prepareResult.getParamCount() - 1, index));
    }
    parameters[index] = Parameter.NULL_PARAMETER;
    return this;
  }

  private int getColumn(String name) {
    for (int i = 0; i < this.prepareResult.getParamNameList().size(); i++) {
      if (name.equals(this.prepareResult.getParamNameList().get(i))) return i;
    }
    throw new IllegalArgumentException(
        String.format(
            "No parameter with name '%s' found (possible values %s)",
            name, this.prepareResult.getParamNameList().toString()));
  }

  @Override
  public Flux<io.r2dbc.mariadb.api.MariadbResult> execute() {

    // valid parameters
    for (int i = 0; i < prepareResult.getParamCount(); i++) {
      if (parameters[i] == null) {
        throw new IllegalArgumentException(String.format("Parameter at position %s is not set", i));
      }
    }

    return execute(this.sql, this.prepareResult, parameters, this.generatedColumns);
  }

  @Override
  public MariadbClientParameterizedQueryStatement fetchSize(int rows) {
    return this;
  }

  @Override
  public MariadbClientParameterizedQueryStatement returnGeneratedValues(String... columns) {
    Assert.requireNonNull(columns, "columns must not be null");

    if (!(client.getVersion().isMariaDBServer()
        && client.getVersion().versionGreaterOrEqual(10, 5, 1))) {
      throw new IllegalStateException(
          "Server does not support RETURNING clause (require MariaDB 10.5.1 version)");
    }
    prepareResult.validateAddingReturning();
    this.generatedColumns = columns;
    return this;
  }

  private Flux<io.r2dbc.mariadb.api.MariadbResult> execute(
      String sql,
      ClientPrepareResult prepareResult,
      Parameter<?>[] parameters,
      String[] generatedColumns) {
    ExceptionFactory factory = ExceptionFactory.withSql(sql);

    Flux<ServerMessage> response =
        this.client.sendCommand(
            new QueryWithParametersPacket(prepareResult, parameters, generatedColumns));

    // separate result-set
    return response
        .windowUntil(it -> it.resultSetEnd())
        .map(dataRow -> new MariadbResult(true, dataRow, factory));
  }

  @Override
  public String toString() {
    return "MariadbClientParameterizedQueryStatement{"
        + "client="
        + client
        + ", sql='"
        + sql
        + '\''
        + ", prepareResult="
        + prepareResult
        + ", parameters="
        + Arrays.toString(parameters)
        + ", configuration="
        + configuration
        + ", generatedColumns="
        + Arrays.toString(generatedColumns)
        + '}';
  }
}
