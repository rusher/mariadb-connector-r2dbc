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
import io.r2dbc.spi.Blob;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;
import java.util.EnumSet;

public class BlobCodec implements Codec<Blob> {

  public static final BlobCodec INSTANCE = new BlobCodec();

  private static EnumSet<DataType> COMPATIBLE_TYPES =
      EnumSet.of(DataType.BLOB, DataType.TINYBLOB, DataType.MEDIUMBLOB, DataType.LONGBLOB);

  public boolean canDecode(ColumnDefinitionPacket column, Class<?> type) {
    return COMPATIBLE_TYPES.contains(column.getDataType()) && type.isAssignableFrom(Blob.class);
  }

  @Override
  public Blob decodeText(ByteBuf buf, ColumnDefinitionPacket column, Class<? extends Blob> type) {

    return new Blob() {
      private ByteBuf buffer = buf.retainedDuplicate();

      @Override
      public Publisher<ByteBuffer> stream() {
        if (buffer == null) {
          throw new IllegalStateException("Data has already been consumed");
        }
        try {
          return Mono.just(buffer.nioBuffer());
        } finally {
          buffer.release();
          buffer = null;
        }
      }

      @Override
      public Publisher<Void> discard() {
        return Mono.fromRunnable(
            () -> {
              if (this.buffer != null && this.buffer.refCnt() > 0) {
                this.buffer.release();
              }
            });
      }
    };
  }

  public boolean canEncode(Object value) {
    return value instanceof Blob;
  }

  @Override
  public void encode(ByteBuf buf, ConnectionContext context, Blob value) {
    BufferUtils.write(buf, value, context);
  }

  @Override
  public String toString() {
    return "BlobCodec{}";
  }
}
