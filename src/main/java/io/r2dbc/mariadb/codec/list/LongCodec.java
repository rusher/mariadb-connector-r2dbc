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

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.EnumSet;

public class LongCodec implements Codec<Long> {

  public static final LongCodec INSTANCE = new LongCodec();

  private static EnumSet<DataType> COMPATIBLE_TYPES =
      EnumSet.of(
          DataType.BIT,
          DataType.TINYINT,
          DataType.SMALLINT,
          DataType.MEDIUMINT,
          DataType.INTEGER,
          DataType.YEAR,
          DataType.BIGINT);

  public static long parse(ByteBuf buf) {
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
    return result;
  }

  public boolean canDecode(ColumnDefinitionPacket column, Class<?> type) {
    return COMPATIBLE_TYPES.contains(column.getDataType())
        && ((type.isPrimitive() && type == Integer.TYPE) || type.isAssignableFrom(Long.class));
  }

  public boolean canEncode(Object value) {
    return value instanceof Long;
  }

  @Override
  public Long decodeText(ByteBuf buf, ColumnDefinitionPacket column, Class<? extends Long> type) {
    switch (column.getDataType()) {
      case BIT:
        return ByteCodec.parseBit(buf);
      case TINYINT:
      case SMALLINT:
      case MEDIUMINT:
      case INTEGER:
      case YEAR:
        return parse(buf);

      case BIGINT:
        String str =
            buf.readCharSequence(buf.readableBytes(), StandardCharsets.US_ASCII).toString();
        return new BigInteger(str).longValueExact();

        //      case DECIMAL:
        //      case DOUBLE:
        //      case FLOAT:
        //        String str = buf.readCharSequence(buf.readableBytes(),
        // StandardCharsets.US_ASCII).toString();
        //        try {
        //          return new BigDecimal(str).longValue();
        //        } catch (NumberFormatException nfe) {
        //          throw new IllegalArgumentException(String.format("Incorrect format %s", str));
        //        }
    }
    throw new IllegalArgumentException(
        String.format("Unexpected datatype %s", column.getDataType()));
  }

  @Override
  public void encode(ByteBuf buf, ConnectionContext context, Long value) {
    BufferUtils.writeAscii(buf, String.valueOf(value));
  }

  @Override
  public String toString() {
    return "LongCodec{}";
  }
}
