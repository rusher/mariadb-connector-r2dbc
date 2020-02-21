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

package org.mariadb.r2dbc;

import io.netty.buffer.ByteBuf;
import org.mariadb.r2dbc.codec.RowDecoder;
import org.mariadb.r2dbc.codec.TextRowDecoder;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import org.mariadb.r2dbc.message.server.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.function.BiFunction;

final class MariadbResult implements org.mariadb.r2dbc.api.MariadbResult {

  private final Flux<ServerMessage> dataRows;
  private final ExceptionFactory factory;
  private final RowDecoder decoder;
  private volatile ColumnDefinitionPacket[] metadataList;
  private volatile int metadataIndex;
  private volatile int columnNumber;
  private volatile MariadbRowMetadata rowMetadata;

  MariadbResult(boolean text, Flux<ServerMessage> dataRows, ExceptionFactory factory) {
    this.dataRows = dataRows;
    this.factory = factory;
    // TODO do binary decoder too
    this.decoder = new TextRowDecoder();
  }

  @Override
  public Mono<Integer> getRowsUpdated() {
    return this.dataRows
        .singleOrEmpty()
        .handle(
            (serverMessage, sink) -> {
              if (serverMessage instanceof ErrorPacket) {
                sink.error(this.factory.from((ErrorPacket) serverMessage));
                return;
              }

              if (serverMessage instanceof OkPacket) {
                OkPacket okPacket = (OkPacket) serverMessage;
                long affectedRows = okPacket.getAffectedRows();
                sink.next((int) affectedRows);
                sink.complete();
              }

              if (serverMessage instanceof RowPacket) {
                // in case of having a resultset
                ((RowPacket) serverMessage).getRaw().release();
              }
            });
  }

  @Override
  public <T> Flux<T> map(BiFunction<Row, RowMetadata, ? extends T> f) {
    metadataIndex = 0;
    return this.dataRows.handle(
        (serverMessage, sink) -> {
          if (serverMessage instanceof ErrorPacket) {
            sink.error(this.factory.from((ErrorPacket) serverMessage));
            return;
          }

          if (serverMessage instanceof ColumnCountPacket) {
            this.columnNumber = ((ColumnCountPacket) serverMessage).getColumnCount();
            metadataList = new ColumnDefinitionPacket[this.columnNumber];
            return;
          }

          if (serverMessage instanceof ColumnDefinitionPacket) {
            this.metadataList[metadataIndex++] = (ColumnDefinitionPacket) serverMessage;
            if (metadataIndex == columnNumber) {
              rowMetadata = MariadbRowMetadata.toRowMetadata(this.metadataList);
            }
            return;
          }

          if (serverMessage instanceof RowPacket) {
            ByteBuf buf = ((RowPacket) serverMessage).getRaw();
            sink.next(f.apply(new MariadbRow(metadataList, decoder, buf), rowMetadata));
            return;
          }

          if (serverMessage.resultSetEnd()) {
            sink.complete();
          }
        });
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("MariadbResult{messages=");
    sb.append(dataRows).append(", factory=").append(factory).append(", metadataList=[");
    if (metadataList == null) {
      sb.append("null");
    } else {
      for (ColumnDefinitionPacket packet : metadataList) {
        sb.append(packet).append(",");
      }
    }
    sb.append("], columnNumber=")
        .append(columnNumber)
        .append(", rowMetadata=")
        .append(rowMetadata)
        .append("}");
    return sb.toString();
  }
}
