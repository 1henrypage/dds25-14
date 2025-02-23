FROM alpine:latest

RUN apk add --no-cache \
    redis \
    curl \
    bash \
    docker-cli

COPY setup-db.sh /setup-db.sh
RUN chmod +x /setup-db.sh

CMD ["/bin/bash", "/setup-db.sh"]