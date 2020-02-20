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

package io.r2dbc.mariadb.codec.list;

import io.netty.buffer.ByteBuf;
import io.r2dbc.mariadb.client.ConnectionContext;
import io.r2dbc.mariadb.codec.Codec;
import io.r2dbc.mariadb.codec.DataType;
import io.r2dbc.mariadb.message.server.ColumnDefinitionPacket;
import io.r2dbc.mariadb.util.BufferUtils;

public class ByteCodec implements Codec<Byte> {

  public static final ByteCodec INSTANCE = new ByteCodec();

  public static long parseBit(ByteBuf buf) {
    if (buf.readableBytes() == 1) {
      return buf.readByte();
    }
    long val = 0;
    do {
      val += ((long) buf.readUnsignedByte()) << (8 * buf.readableBytes());
    } while (buf.readableBytes() > 0);
    return val;
  }

  public boolean canDecode(ColumnDefinitionPacket column, Class<?> type) {
    return column.getDataType() == DataType.BIT
        && ((type.isPrimitive() && type == Byte.TYPE) || type.isAssignableFrom(Byte.class));
  }

  @Override
  public Byte decodeText(ByteBuf buf, ColumnDefinitionPacket column, Class<? extends Byte> type) {
    if (buf.readableBytes() == 0) {
      throw new IllegalArgumentException(
          String.format("Unexpected datatype %s", column.getDataType()));
    }
    return buf.readByte();
  }

  public boolean canEncode(Object value) {
    return value instanceof Byte;
  }

  @Override
  public void encode(ByteBuf buf, ConnectionContext context, Byte value) {
    BufferUtils.writeAscii(buf, Integer.toString((int) value));
  }

  @Override
  public String toString() {
    return "ByteCodec{}";
  }
}
