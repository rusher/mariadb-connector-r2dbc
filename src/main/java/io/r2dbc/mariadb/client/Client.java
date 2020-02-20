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

package io.r2dbc.mariadb.client;

import io.r2dbc.mariadb.MariadbConnectionConfiguration;
import io.r2dbc.mariadb.message.client.ClientMessage;
import io.r2dbc.mariadb.message.client.SslRequestPacket;
import io.r2dbc.mariadb.message.server.InitialHandshakePacket;
import io.r2dbc.mariadb.message.server.ServerMessage;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.concurrent.locks.ReentrantLock;

public interface Client {

  Mono<Void> close();

  Flux<ServerMessage> receive();

  Flux<ServerMessage> sendCommand(ClientMessage requests);

  Mono<Void> sendSslRequest(
      SslRequestPacket sslRequest, MariadbConnectionConfiguration configuration);

  ClientImpl.LockAction getLockAction();

  boolean isAutoCommit();

  boolean noBackslashEscapes();

  ServerVersion getVersion();

  boolean isConnected();

  void setContext(InitialHandshakePacket packet);
}