#
# 1.--- Build IDSA connector ---------------------------------------------------
#
FROM maven:3-eclipse-temurin-17 AS builder

# Get source code
WORKDIR /app

# When master branch is broken, try using --branch v8.0.2
RUN git clone --single-branch --branch v8.0.2 https://github.com/International-Data-Spaces-Association/DataspaceConnector.git .

# Resolve dependencies
RUN mvn -e -B dependency:resolve && \
    mvn -e -B dependency:resolve-plugins

# Build
RUN mvn -e -B clean package -DskipTests -Dmaven.javadoc.skip=true && \
    java -Djarmode=layertools -jar /app/target/dataspaceconnector.jar extract


#
# 2.--- Copy connector, certs, config to final container  ---------------------
#
FROM eclipse-temurin:17-jre-alpine

RUN apk update && \
    apk add bash gettext

# Copy the jar and build image
WORKDIR /app
COPY --from=builder /app/spring-boot-loader/ ./
COPY --from=builder /app/dependencies/ ./
COPY --from=builder /app/application/ ./

# Prepare config
COPY conf/* conf/
COPY entrypoint.sh .
RUN chmod a+x entrypoint.sh

EXPOSE 8080
EXPOSE 29292

ENTRYPOINT ["/app/entrypoint.sh"]
