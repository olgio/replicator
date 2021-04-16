FROM openjdk:15
COPY target/replicator-runner-DEV-SNAPSHOT-jar-with-dependencies.jar /app.jar
COPY benchmark/start.sh /start.sh
RUN ["chmod", "+x", "/start.sh"]
ENTRYPOINT ["/start.sh"]
#EXPOSE 8000
#EXPOSE 6545
#CMD ["java", "-Xms1536M", "-Xmx1536M", "-cp", "/app.jar", "ru.splite.replicator.benchmark.BenchmarkKt", "--n", "5", "--f", "2", "--keys", "30", "--threads", "5"]