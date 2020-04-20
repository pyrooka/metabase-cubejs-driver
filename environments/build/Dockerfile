FROM openjdk:11-jdk

ADD https://raw.github.com/technomancy/leiningen/stable/bin/lein /usr/local/bin/lein

RUN chmod 744 /usr/local/bin/lein && \
    git clone https://github.com/metabase/metabase.git --branch=release-0.35.x && \
    cd metabase && \
    lein install-for-building-drivers

WORKDIR /driver/metabase-cubejs-driver