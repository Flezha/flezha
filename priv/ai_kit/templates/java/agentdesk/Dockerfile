# The runner's image: the jar `mvn -q package` builds, on a Java 21 runtime.
FROM eclipse-temurin:21-jre
COPY target/agentdesk.jar /agentdesk/agentdesk.jar
USER 1000
ENTRYPOINT ["java", "-jar", "/agentdesk/agentdesk.jar"]
