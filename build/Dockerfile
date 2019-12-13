FROM openjdk:8-jdk-alpine

RUN apk add --update git bash

ADD https://raw.github.com/technomancy/leiningen/stable/bin/lein /usr/local/bin/lein
RUN chmod 744 /usr/local/bin/lein

RUN git clone https://github.com/metabase/metabase.git --branch=v0.33.6

WORKDIR /metabase
RUN lein install-for-building-drivers

WORKDIR /driver/metabase-cubejs-driver