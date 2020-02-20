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

package io.r2dbc.mariadb.codec;

public class TextRowDecoder extends RowDecoder {

  public TextRowDecoder() {
    super();
  }

  /**
   * Set length and pos indicator to asked index.
   *
   * @param newIndex index (0 is first).
   */
  public void setPosition(int newIndex) {
    if (index != newIndex) {
      if (index > newIndex) {
        index = 0;
        buf.resetReaderIndex();
      } else {
        index++;
      }

      for (; index < newIndex; index++) {
        int type = this.buf.readUnsignedByte();
        switch (type) {
          case 252:
            buf.skipBytes(buf.readUnsignedShortLE());
            break;
          case 253:
            buf.skipBytes(buf.readUnsignedMediumLE());
            break;
          case 254:
            buf.skipBytes((int) (4 + buf.readUnsignedIntLE()));
            break;
          default:
            buf.skipBytes(type);
            break;
        }
      }
      short type = this.buf.readUnsignedByte();
      switch (type) {
        case 251:
          length = NULL_LENGTH;
          break;
        case 252:
          length = buf.readUnsignedShortLE();
          break;
        case 253:
          length = buf.readUnsignedMediumLE();
          break;
        case 254:
          length = (int) buf.readUnsignedIntLE();
          buf.skipBytes(4);
          break;
        default:
          length = type;
          break;
      }
    }
  }
}
