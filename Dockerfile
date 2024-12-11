# Use OpenJDK 21 as the base image
FROM openjdk:21-jdk-slim

# Set the working directory
WORKDIR /app

# Copy Gradle wrapper and build configuration files
COPY gradlew .
COPY gradle gradle
COPY build.gradle .
COPY settings.gradle .

# Copy gradle.properties
COPY gradle.properties .

# Copy source code
COPY src src

# Ensure Gradle Wrapper has the right permissions
RUN chmod +x gradlew

# Pre-download dependencies
RUN ./gradlew dependencies --no-daemon

# Build the project, skipping tests
RUN ./gradlew clean build -x test --no-daemon

# Expose the application port
EXPOSE 8080

# Set the default command to run the application using wait-for-it
CMD ["db", "--", "java", "-jar", "build/libs/kafka-web-1.0.0.jar"]