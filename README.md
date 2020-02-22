<p align="center">
  <a href="http://mariadb.com/">
    <img src="https://mariadb.com/kb/static/images/logo-2018-black.png">
  </a>
</p>

# MariaDB R2DBC connector

[![Maven Central][maven-image]][maven-url]
[![Linux Build][travis-image]][travis-url]
[![Windows status][appveyor-image]][appveyor-url]
[![License][license-image]][license-url]


**Non-blocking MariaDB and MySQL client.**

MariaDB and MySQL client, 100% Java, compatible with Java8+, apache 2.0 licensed.
- Driver permits ed25519, PAM authentication that comes with MariaDB.
- use MariaDB 10.5 returning fonction to permit Statement.returnGeneratedValues 

Driver follow [R2DBC 0.8.1 specifications](https://r2dbc.io/spec/0.8.1.RELEASE/spec/html/)


## Quick Start

The MariaDB Connector is available through maven using :

```
    <dependency>
        <groupId>org.mariadb</groupId>
        <artifactId>r2dbc-mariadb</artifactId>
        <version>0.8.1-alpha1</version>
    </dependency>
```

Factory can be created using ConnectionFactory or using connection URL.

Using builder                                                     
```java

MariadbConnectionConfiguration conf = MariadbConnectionConfiguration.builder()
        .host(host)
        .port(port)
        .username(username)
        .password(password)
        .database(database).build();
MariadbConnectionFactory factory = new MariadbConnectionFactory(conf);

//OR

ConnectionFactory factory = ConnectionFactories.get("r2dbc:mariadb://user:pswword@host:3306/myDB?option1=value");
```



```java
    MariadbConnectionConfiguration conf = MariadbConnectionConfiguration.builder()
            .host(host)
            .port(port)
            .username(username)
            .password(password)
            .database(database).build();
    MariadbConnectionFactory factory = new MariadbConnectionFactory(conf);

    MariadbConnection connection = factory.create().block();
    connection.createStatement("SELECT * FROM myTable")
            .execute()
            .flatMap(r -> r.map((row, metadata) -> {
              return "value=" + row.get(0, String.class);
            }));
    connection.close().subscribe();
```

### Connection options

|option|description|type|default| 
|---:|---|:---:|:---:| 
| **`username`** | User to access database. |*string* | 
| **`password`** | User password. |*string* | 
| **`host`** | IP address or DNS of the database server. *Not used when using option `socketPath`*. |*string*| "localhost"|
| **`port`** | Database server port number. *Not used when using option `socketPath`*|*integer*| 3306|
| **`database`** | Default database to use when establishing the connection. | *string* | 
| **`connectTimeout`** | Sets the connection timeout in milliseconds. Default to 10s. |  *Duration* | 
| **`socket`** | Permits connections to the database through the Unix domain socket for faster connection whe server is local. |  *string* | 
| **`allowMultiQueries`** | Allows you to issue several SQL statements in a single `quer()` call. (That is, `INSERT INTO a VALUES('b'); INSERT INTO c VALUES('d');`).  <br/><br/>This may be a **security risk** as it allows for SQL Injection attacks. Default to false|  *boolean* | 
| **`connectionAttributes`** | When performance_schema is active, permit to send server some client information. Those informations can be retrieved on server within tables performance_schema.session_connect_attrs and performance_schema.session_account_connect_attrs. This can permit from server an identification of client/application per connection|*Map<String,String>* | 
| **`sessionVariables`** | Permits to set session variables upon successful connection |  *Map<String,String>* |
| **`tlsProtocol`** |Force TLS/SSL protocol to a specific set of TLS versions (like "TLSv1.3","TLSv1.2"|*List<String>*|
| **`serverSslCert`** | Permits providing server's certificate in DER form, or server's CA certificate. This permits a self-signed certificate to be trusted. Can be used in one of 3 forms : * serverSslCert=/path/to/cert.pem (full path to certificate), serverSslCert=classpath:relative/cert.pem (relative to current classpath) or as verbatim DER-encoded certificate string "------BEGIN CERTIFICATE-----" |*String*| |
| **`clientSslCert`** | Permits providing client's certificate in DER form (use only for mutual authentication). Can be used in one of 3 forms : * serverSslCert=/path/to/cert.pem (full path to certificate), serverSslCert=classpath:relative/cert.pem (relative to current classpath) or as verbatim DER-encoded certificate string "------BEGIN CERTIFICATE-----" |*String*| |
| **`clientSslKey`** | client private key path(for mutual authentication) |*String* | |
| **`clientSslPassword`** | client private key password |*charsequence* | |
| **`sslMode`** | ssl requirement. Possible value are <ul><li>DISABLED, // NO SSL</li><li>ENABLE_TRUST, // Encryption, but no certificate and hostname validation  (DEVELOPMENT ONLY)<\li><li>ENABLE_WITHOUT_HOSTNAME_VERIFICATION, // Encryption, certificates validation, BUT no hostname validation<\li><li>ENABLE, // Standard SSL use: Encryption, certificate validation and hostname validation<\li></ul> | DISABLED |
| **`serverRsaPublicKeyFile`** | <i>only for MySQL server</i> Server RSA public key, for SHA256 authentication |*String* | |
| **`allowPublicKeyRetrieval`** | <i>only for MySQL server</i> Permit retrieved Server RSA public key from server. This can create a security issue |*boolean* | | 
      

## Tracker 

To file an issue or follow the development, see [JIRA](https://jira.mariadb.org/projects/R2DBC/issues/).


[travis-image]:https://travis-ci.org/mariadb-corporation/mariadb-connector-nodejs.svg?branch=master
[travis-url]:https://travis-ci.org/mariadb-corporation/mariadb-connector-nodejs
[maven-image]:https://maven-badges.herokuapp.com/maven-central/org.mariadb/r2dbc-mariadb/badge.svg
[maven-url]:https://maven-badges.herokuapp.com/maven-central/org.mariadb/r2dbc-mariadb
[appveyor-image]:https://ci.appveyor.com/api/projects/status/558kpv0j1r545pgq/branch/master?svg=true
[appveyor-url]:https://ci.appveyor.com/project/rusher/mariadb-connector-nodejs-w8k25
[license-image]:https://img.shields.io/badge/License-Apache%202.0-blue.svg
[license-url]:https://opensource.org/licenses/Apache-2.0
