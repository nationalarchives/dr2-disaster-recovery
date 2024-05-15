FROM alpine
COPY target/scala-3.4.1/dr2-disaster-recovery.jar /dr2-disaster-recovery.jar
RUN apk update && apk upgrade && apk add openjdk19-jre && \
    mkdir -p /poduser/work /poduser/repo /poduser/version && \
    chown -R 1002:1005 /poduser && \
    mkdir /poduser/logs && \
    touch /poduser/logs/disaster-recovery.log && \
    chown -R nobody:nobody /poduser/logs && \
    chmod 644 /poduser/logs/disaster-recovery.log
USER 1002
RUN ls -la /poduser/logs
CMD java -jar /dr2-disaster-recovery.jar
