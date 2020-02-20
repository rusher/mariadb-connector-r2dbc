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

public class TinyIntCodec implements Codec<Byte> {

  public static final TinyIntCodec INSTANCE = new TinyIntCodec();

  public boolean canDecode(ColumnDefinitionPacket column, Class<?> type) {
    return column.getDataType() == DataType.TINYINT
        && ((type.isPrimitive() && type == Byte.TYPE) || type.isAssignableFrom(Byte.class));
  }

  public boolean canEncode(Object value) {
    return value instanceof Byte;
  }

  @Override
  public Byte decodeText(ByteBuf buf, ColumnDefinitionPacket column, Class<? extends Byte> type) {
    long result = 0;
    boolean negate = false;
    if (buf.readableBytes() > 0 && buf.getByte(buf.readerIndex()) == 45) { // minus sign
      negate = true;
      buf.skipBytes(1);
    }
    while (buf.readableBytes() > 0) {
      result = result * 10 + buf.readByte() - 48;
    }

    if (negate) result = -1 * result;
    IntCodec.rangeCheck(Byte.class.getName(), Byte.MIN_VALUE, Byte.MAX_VALUE, result, column);
    return (byte) result;
  }

  @Override
  public void encode(ByteBuf buf, ConnectionContext context, Byte value) {
    BufferUtils.writeAscii(buf, String.valueOf(value));
  }

  @Override
  public String toString() {
    return "TinyIntCodec{}";
  }
}