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

package io.r2dbc.mariadb.message.flow;

import io.r2dbc.mariadb.MariadbConnectionConfiguration;
import io.r2dbc.mariadb.authentication.AuthenticationPlugin;
import io.r2dbc.mariadb.message.client.ClientMessage;
import io.r2dbc.mariadb.message.client.NativePasswordPacket;
import io.r2dbc.mariadb.message.server.AuthMoreDataPacket;
import io.r2dbc.mariadb.message.server.AuthSwitchPacket;

public final class NativePasswordPluginFlow implements AuthenticationPlugin {

  public static final String TYPE = "mysql_native_password";

  public String type() {
    return TYPE;
  }

  public ClientMessage next(
      MariadbConnectionConfiguration configuration,
      AuthSwitchPacket authSwitchPacket,
      AuthMoreDataPacket authMoreDataPacket) {
    return new NativePasswordPacket(
        authSwitchPacket.getSequencer(), configuration.getPassword(), authSwitchPacket.getSeed());
  }

  @Override
  public String toString() {
    return "NativePasswordPluginFlow{}";
  }
}
