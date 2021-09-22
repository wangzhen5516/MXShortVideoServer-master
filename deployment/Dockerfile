FROM maven:3.6-jdk-8-alpine as builder

ADD repo /srv/repo
ADD .m2 /root/.m2

WORKDIR /srv/repo

RUN rm -rf .git && \
    sed -i 's/function//g' build.sh && \
    sed -i 's/ensure_success/ensure_success\(\)/g' build.sh && \
    sed -i '/npm/d' build.sh && \
    sh build.sh && \
    rm -rf /srv/repo/target/classes && \
    rm -rf /srv/repo/target/generated* && \
    rm -rf /srv/repo/target/maven* && \
    rm -rf /srv/repo/src/main/java && \
    rm -rf /var/lib/apt/lists/*

FROM alpine:3.14

ENV DEPLOY_ENV not_defined
ENV CONFIG_URL not_defined
ENV SERVICE_NAME not_defined
ENV PORT not_defined

COPY --from=builder /srv/repo /srv/repo

WORKDIR /srv/repo

RUN apk --update --no-cache -q upgrade && \
    apk --update --no-cache -q add openjdk8=8.282.08-r1 tini curl && \ 
    addgroup -S app && \
    adduser -S app -G app && \
    chown -R app:app /srv/repo && \
    rm -rf /var/cache/apk/*

USER app

ENTRYPOINT ["tini", "--", "sh", "deployment/entrypoint.sh"]

CMD ["sh", "deployment/startup.sh"]
