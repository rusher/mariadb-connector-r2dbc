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
import io.r2dbc.mariadb.SslMode;
import io.r2dbc.mariadb.message.client.*;
import io.r2dbc.mariadb.message.server.AuthMoreDataPacket;
import io.r2dbc.mariadb.message.server.AuthSwitchPacket;
import io.r2dbc.spi.R2dbcException;
import io.r2dbc.spi.R2dbcNonTransientResourceException;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.PublicKey;

public final class CachingSha2PasswordFlow extends Sha256PasswordPluginFlow {

  public static final String TYPE = "caching_sha2_password";
  private State state = State.INIT;
  private PublicKey publicKey;

  /**
   * Send a SHA-2 encrypted password. encryption XOR(SHA256(password), SHA256(seed,
   * SHA256(SHA256(password))))
   *
   * @param password password
   * @param seed seed
   * @return encrypted pwd
   */
  public static byte[] sha256encryptPassword(final CharSequence password, final byte[] seed) {

    if (password == null || password.length() == 0) {
      return new byte[0];
    }
    try {
      final MessageDigest messageDigest = MessageDigest.getInstance("SHA-256");
      byte[] bytePwd = password.toString().getBytes(StandardCharsets.UTF_8);

      final byte[] stage1 = messageDigest.digest(bytePwd);
      messageDigest.reset();

      final byte[] stage2 = messageDigest.digest(stage1);
      messageDigest.reset();

      messageDigest.update(stage2);
      messageDigest.update(seed);

      final byte[] digest = messageDigest.digest();
      final byte[] returnBytes = new byte[digest.length];
      for (int i = 0; i < digest.length; i++) {
        returnBytes[i] = (byte) (stage1[i] ^ digest[i]);
      }
      return returnBytes;
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException("Could not use SHA-256, failing", e);
    }
  }

  public String type() {
    return TYPE;
  }

  public ClientMessage next(
      MariadbConnectionConfiguration configuration,
      AuthSwitchPacket authSwitchPacket,
      AuthMoreDataPacket authMoreDataPacket)
      throws R2dbcException {

    if (authMoreDataPacket == null) state = State.INIT;

    CharSequence password = configuration.getPassword();
    switch (state) {
      case INIT:
        byte[] fastCryptedPwd = sha256encryptPassword(password, authSwitchPacket.getSeed());
        state = State.FAST_AUTH_RESULT;
        return new AuthMoreRawPacket(authSwitchPacket.getSequencer(), fastCryptedPwd);

      case FAST_AUTH_RESULT:
        byte fastAuthResult = authMoreDataPacket.getBuf().getByte(0);
        switch (fastAuthResult) {
          case 3:
            // success authentication
            return null;

          case 4:
            if (configuration.getSslConfig().getSslMode() != SslMode.DISABLED) {
              // send clear password
              state = State.SEND_AUTH;
              return new ClearPasswordPacket(authMoreDataPacket.getSequencer(), password);
            }

            // retrieve public key from configuration or from server
            if (configuration.getServerRsaPublicKeyFile() != null
                && !configuration.getServerRsaPublicKeyFile().isEmpty()) {
              publicKey = readPublicKeyFromFile(configuration.getServerRsaPublicKeyFile());
              state = State.SEND_AUTH;
              return new Sha256PasswordPacket(
                  authMoreDataPacket.getSequencer(),
                  configuration.getPassword(),
                  authSwitchPacket.getSeed(),
                  publicKey);
            }

            if (!configuration.isAllowPublicKeyRetrieval()) {
              throw new R2dbcNonTransientResourceException(
                  "RSA public key is not available client side (option "
                      + "serverRsaPublicKeyFile)",
                  "S1009");
            }

            state = State.REQUEST_SERVER_KEY;
            // ask public Key Retrieval
            return new Sha2PublicKeyRequestPacket(authMoreDataPacket.getSequencer());

          default:
            throw new R2dbcNonTransientResourceException(
                "Protocol exchange error. Expect login success or RSA login request message",
                "S1009");
        }

      case REQUEST_SERVER_KEY:
        publicKey = readPublicKey(authMoreDataPacket);
        state = State.SEND_AUTH;
        return new Sha256PasswordPacket(
            authMoreDataPacket.getSequencer(),
            configuration.getPassword(),
            authSwitchPacket.getSeed(),
            publicKey);

      default:
        throw new R2dbcNonTransientResourceException("Wrong state", "S1009");
    }
  }

  @Override
  public String toString() {
    return "CachingSha2PasswordFlow{}";
  }

  public enum State {
    INIT,
    FAST_AUTH_RESULT,
    REQUEST_SERVER_KEY,
    SEND_AUTH,
  }
}
