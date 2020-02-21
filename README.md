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
[![codecov][codecov-image]][codecov-url]

**Non-blocking MariaDB and MySQL client.**

MariaDB and MySQL client, 100% Java, compatible with Java8+.
- Driver permits ed25519, PAM authentication that comes with MariaDB.
- use MariaDB 10.5 returning fonction to permit Statement.returnGeneratedValues 

## Quick Start

The MariaDB Connector is available through the maven.js repositories.  You can install it using npm :

```
$ npm install mariadb
```

Using ECMAScript < 2017:

```js
const mariadb = require('mariadb');
const pool = mariadb.createPool({host: process.env.DB_HOST, user: process.env.DB_USER, connectionLimit: 5});
pool.getConnection()
    .then(conn => {
    
      conn.query("SELECT 1 as val")
        .then(rows => { // rows: [ {val: 1}, meta: ... ]
          return conn.query("INSERT INTO myTable value (?, ?)", [1, "mariadb"]);
        })
        .then(res => { // res: { affectedRows: 1, insertId: 1, warningStatus: 0 }
          conn.release(); // release to pool
        })
        .catch(err => {
          conn.release(); // release to pool
        })
        
    }).catch(err => {
      //not connected
    });
```

Using ECMAScript 2017:

```js
const mariadb = require('mariadb');
const pool = mariadb.createPool({host: process.env.DB_HOST, user: process.env.DB_USER, connectionLimit: 5});

async function asyncFunction() {
  let conn;
  try {

	conn = await pool.getConnection();
	const rows = await conn.query("SELECT 1 as val");
	// rows: [ {val: 1}, meta: ... ]

	const res = await conn.query("INSERT INTO myTable value (?, ?)", [1, "mariadb"]);
	// res: { affectedRows: 1, insertId: 1, warningStatus: 0 }

  } catch (err) {
	throw err;
  } finally {
	if (conn) conn.release(); //release to pool
  }
}
```

## Contributing 

If you would like to contribute to the MariaDB Node.js Connector, please follow the instructions given in the [Developers Guide.](/documentation/developers-guide.md)

To file an issue or follow the development, see [JIRA](https://jira.mariadb.org/projects/CONJS/issues/).


[travis-image]:https://travis-ci.org/mariadb-corporation/mariadb-connector-nodejs.svg?branch=master
[travis-url]:https://travis-ci.org/mariadb-corporation/mariadb-connector-nodejs
[maven-image]:https://maven-badges.herokuapp.com/maven-central/org.mariadb/r2dbc-mariadb/badge.svg
[maven-url]:https://maven-badges.herokuapp.com/maven-central/org.mariadb/r2dbc-mariadb
[appveyor-image]:https://ci.appveyor.com/api/projects/status/558kpv0j1r545pgq/branch/master?svg=true
[appveyor-url]:https://ci.appveyor.com/project/rusher/mariadb-connector-nodejs-w8k25
[license-image]:https://img.shields.io/badge/License-Apache%202.0-blue.svg
[license-url]:https://opensource.org/licenses/Apache-2.0
[codecov-image]:https://codecov.io/gh/mariadb-corporation/mariadb-connector-nodejs/branch/master/graph/badge.svg
[codecov-url]:https://codecov.io/gh/mariadb-corporation/mariadb-connector-nodejs