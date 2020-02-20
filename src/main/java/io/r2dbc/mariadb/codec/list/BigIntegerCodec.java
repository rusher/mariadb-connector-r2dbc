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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.EnumSet;

public class BigIntegerCodec implements Codec<BigInteger> {

  public static final BigIntegerCodec INSTANCE = new BigIntegerCodec();

  private static EnumSet<DataType> COMPATIBLE_TYPES =
      EnumSet.of(
          DataType.BIT,
          DataType.TINYINT,
          DataType.SMALLINT,
          DataType.MEDIUMINT,
          DataType.INTEGER,
          DataType.BIGINT,
          DataType.DECIMAL,
          DataType.YEAR);

  public boolean canDecode(ColumnDefinitionPacket column, Class<?> type) {
    return COMPATIBLE_TYPES.contains(column.getDataType())
        && type.isAssignableFrom(BigInteger.class);
  }

  public boolean canEncode(Object value) {
    return value instanceof BigInteger;
  }

  @Override
  public BigInteger decodeText(
      ByteBuf buf, ColumnDefinitionPacket column, Class<? extends BigInteger> type) {
    switch (column.getDataType()) {
      case BIT:
        return BigInteger.valueOf(ByteCodec.parseBit(buf));
      case DECIMAL:
        String value = buf.readCharSequence(buf.readableBytes(), StandardCharsets.UTF_8).toString();
        return new BigDecimal(value).toBigInteger();

      case TINYINT:
      case SMALLINT:
      case MEDIUMINT:
      case INTEGER:
      case BIGINT:
      case YEAR:
        String val = buf.readCharSequence(buf.readableBytes(), StandardCharsets.UTF_8).toString();
        return new BigInteger(val);
    }
    throw new IllegalArgumentException(
        String.format("Unexpected datatype %s", column.getDataType()));
  }

  @Override
  public void encode(ByteBuf buf, ConnectionContext context, BigInteger value) {
    BufferUtils.writeAscii(buf, value.toString());
  }

  @Override
  public String toString() {
    return "BigIntegerCodec{}";
  }
}
