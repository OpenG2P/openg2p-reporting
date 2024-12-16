FROM alpine:20240923

ARG container_user_id=1001
ARG container_group_id=1001
ARG container_user=openg2p
ARG container_group=openg2p

RUN apk add bash vim git curl jq yq && \
    apk add gettext kubectl helm postgresql-client

RUN addgroup -S ${container_group} -g ${container_group_id} && \
    adduser -S ${container_user} -G ${container_group} -u ${container_user_id}

RUN mkdir /reporting-init && \
    chown -R openg2p:openg2p /reporting-init

WORKDIR /reporting-init

USER ${container_user}

CMD [ "bash" ]
