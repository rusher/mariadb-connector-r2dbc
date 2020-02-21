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

package org.mariadb.r2dbc.integration.authentication;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mariadb.r2dbc.BaseTest;
import org.mariadb.r2dbc.MariadbConnectionConfiguration;
import org.mariadb.r2dbc.MariadbConnectionFactory;
import org.mariadb.r2dbc.TestConfiguration;
import org.mariadb.r2dbc.api.MariadbConnection;
import org.mariadb.r2dbc.api.MariadbConnectionMetadata;
import reactor.core.publisher.Mono;

public class Ed25519PluginTest extends BaseTest {

  @BeforeAll
  public static void before2() {
    MariadbConnectionMetadata meta = sharedConn.getMetadata();
    if (meta.isMariaDBServer() && meta.minVersion(10, 2, 0)) {
      sharedConn
          .createStatement("INSTALL SONAME 'auth_ed25519'")
          .execute()
          .map(res -> res.getRowsUpdated())
          .onErrorReturn(Mono.empty())
          .blockLast();
      if (meta.minVersion(10, 4, 0)) {
        sharedConn
            .createStatement(
                "CREATE USER verificationEd25519AuthPlugin IDENTIFIED "
                    + "VIA ed25519 USING PASSWORD('MySup8%rPassw@ord')")
            .execute()
            .subscribe();
      } else {
        sharedConn
            .createStatement(
                "CREATE USER verificationEd25519AuthPlugin IDENTIFIED "
                    + "VIA ed25519 USING '6aW9C7ENlasUfymtfMvMZZtnkCVlcb1ssxOLJ0kj/AA'")
            .execute()
            .subscribe();
      }
      sharedConn
          .createStatement("GRANT ALL on *.* to verificationEd25519AuthPlugin")
          .execute()
          .blockLast();
    }
  }

  @AfterAll
  public static void after2() {
    MariadbConnectionMetadata meta = sharedConn.getMetadata();
    if (meta.isMariaDBServer() && meta.minVersion(10, 2, 0)) {
      sharedConn
          .createStatement("DROP USER verificationEd25519AuthPlugin")
          .execute()
          .map(res -> res.getRowsUpdated())
          .onErrorReturn(Mono.empty())
          .subscribe();
    }
  }

  @Test
  public void verificationEd25519AuthPlugin() throws Throwable {
    MariadbConnectionMetadata meta = sharedConn.getMetadata();
    Assumptions.assumeTrue(meta.isMariaDBServer() && meta.minVersion(10, 2, 0));

    MariadbConnectionConfiguration conf =
        TestConfiguration.defaultBuilder
            .clone()
            .username("verificationEd25519AuthPlugin")
            .password("MySup8%rPassw@ord")
            .build();
    MariadbConnection connection = new MariadbConnectionFactory(conf).create().block();
    connection.close();
  }
}
