# Build stage 0
ARG VERSION=latest
FROM heyoka/balena-erlang-alpine:$VERSION AS alpine

# Set working directory
#RUN mkdir -p /buildroot/rebar3/bin
WORKDIR /buildroot/rebar3/bin
WORKDIR /buildroot

# Copy our Erlang test application
COPY ./ faxe

# And build the release
WORKDIR faxe

## balena cross-build start
RUN [ "cross-build-start" ]
# need git
RUN apk update && apk add bash
SHELL ["/bin/bash", "-c"]
RUN apk add --no-cache git

RUN rebar3 as prod release

# Build stage 1
FROM alpine

# Install some libs
RUN apk add --no-cache openssl && \
    apk add --no-cache ncurses-libs

RUN [ "cross-build-start" ]
## balena cross-build end


ENV RELX_REPLACE_OS_VARS true

# Install the released application
COPY --from=0 /buildroot/faxe/_build/prod/rel/faxe /faxe

# Expose relevant ports
## http api
EXPOSE 8081
EXPOSE 102
EXPOSE 502
EXPOSE 1883
EXPOSE 8883

ENTRYPOINT ["/faxe/bin/faxe"]
# todo: find out why "start" does not work
CMD ["foreground"]