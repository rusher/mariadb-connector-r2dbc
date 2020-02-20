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

package io.r2dbc.mariadb.message.server;

import io.netty.buffer.ByteBuf;
import io.r2dbc.mariadb.client.ConnectionContext;
import io.r2dbc.mariadb.util.BufferUtils;
import reactor.util.Logger;
import reactor.util.Loggers;

import java.util.Objects;

public class ColumnCountPacket implements ServerMessage {
  private static final Logger logger = Loggers.getLogger(ColumnCountPacket.class);

  private int columnCount;

  public ColumnCountPacket(int columnCount) {
    this.columnCount = columnCount;
  }

  public static ColumnCountPacket decode(
      Sequencer sequencer, ByteBuf buf, ConnectionContext context) {
    long columnCount = BufferUtils.readLengthEncodedInt(buf);
    return new ColumnCountPacket((int) columnCount);
  }

  public int getColumnCount() {
    return columnCount;
  }

  @Override
  public Sequencer getSequencer() {
    return null;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ColumnCountPacket that = (ColumnCountPacket) o;
    return columnCount == that.columnCount;
  }

  @Override
  public int hashCode() {
    return Objects.hash(columnCount);
  }

  @Override
  public String toString() {
    return "ColumnCountPacket{" + "columnCount=" + columnCount + '}';
  }
}
