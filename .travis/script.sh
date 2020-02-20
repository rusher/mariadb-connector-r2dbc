#!/bin/bash

set -x
set -e

###################################################################################################################
# test different type of configuration
###################################################################################################################
export TEST_HOST = mariadb.example.com
export TEST_PORT = 3305
export TEST_USERNAME = bob
export TEST_DATABASE = testr2

cmd=(mvn clean test $ADDITIONNAL_VARIABLES -DjobId=${TRAVIS_JOB_ID} \
  -DkeystorePath="$SSLCERT/client-keystore.jks" \
  -DkeystorePassword="kspass" \
  -DserverCertificatePath="$SSLCERT/server.crt" \
  -Dkeystore2Path="$SSLCERT/fullclient-keystore.jks" \
  -Dkeystore2Password="kspass" -DkeyPassword="kspasskey" \
  -Dkeystore2PathP12="$SSLCERT/fullclient-keystore.p12" \
  -DrunLongTest=true \
  -DserverPublicKey="$SSLCERT/public.key")

export INNODB_LOG_FILE_SIZE=$(echo ${PACKET} | cut -d'M' -f 1)0M

if [ -n "$MAXSCALE_VERSION" ]; then
  ###################################################################################################################
  # launch Maxscale with one server
  ###################################################################################################################
  export TEST_PORT = 4007
  mysql=(mysql --protocol=tcp -ubob -h127.0.0.1 --port=4007)
  export COMPOSE_FILE=.travis/maxscale-compose.yml
  docker-compose -f ${COMPOSE_FILE} build
else
  ###################################################################################################################
  # launch docker server
  ###################################################################################################################
  mysql=(mysql --protocol=tcp -ubob -h127.0.0.1 --port=3305)
  export COMPOSE_FILE=.travis/docker-compose.yml
fi

docker-compose -f ${COMPOSE_FILE} up -d

###################################################################################################################
# wait for docker initialisation
###################################################################################################################

for i in {60..0}; do
  if echo 'SELECT 1' | "${mysql[@]}" &>/dev/null; then
    break
  fi
  echo 'data server still not active'
  sleep 1
done

if [ "$i" = 0 ]; then
  if [ -n "COMPOSE_FILE" ]; then
    docker-compose -f ${COMPOSE_FILE} logs
  fi

  echo 'SELECT 1' | "${mysql[@]}"
  echo >&2 'data server init process failed.'
  exit 1
fi

###################################################################################################################
# run test suite
###################################################################################################################
echo "Running coveralls for JDK version: $TRAVIS_JDK_VERSION"
echo ${cmd}

if [ -n "$MAXSCALE_VERSION" ]; then
  docker-compose -f $COMPOSE_FILE exec maxscale tail -n 500 /var/log/maxscale/maxscale.log
fi

"${cmd[@]}"
