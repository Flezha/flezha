# Flezha Java SDK

Java SDK for interacting with the Flezha platform.

## Prerequisites

- Java 21 or higher
- Gradle (included via Gradle wrapper)

## Building the Project

To build the project, run:

```bash
./gradlew build
```

This will compile the code, run tests, and create the build artifacts in the `build/` directory.

## Running Tests

To run the test suite:

```bash
./gradlew test
```

To run tests with verbose output:

```bash
./gradlew test --info
```

To run tests continuously (useful during development):

```bash
./gradlew test --continuous
```

## Running the SDK

To run the main application:

```bash
./gradlew run
```

Alternatively, you can build and run the JAR directly:

```bash
./gradlew build
java -jar build/libs/java.jar
```

## Development

### Clean Build Artifacts

To clean all build artifacts:

```bash
./gradlew clean
```

### Rebuild from Scratch

```bash
./gradlew clean build
```

### Check Dependencies

To see the dependency tree:

```bash
./gradlew dependencies
```

## Project Structure

```
sdk/java/
├── src/
│   ├── main/
│   │   └── java/
│   │       └── com/
│   │           └── flezha/
│   │               └── sdk/
│   │                   └── Main.java
│   └── test/
│       └── java/
│           └── com/
│               └── flezha/
│                   └── sdk/
├── build.gradle.kts
├── settings.gradle
└── README.md
```

## Gradle Tasks

Common Gradle tasks:

- `./gradlew build` - Build the project
- `./gradlew test` - Run tests
- `./gradlew run` - Run the application
- `./gradlew clean` - Clean build artifacts
- `./gradlew jar` - Build JAR file
- `./gradlew tasks` - List all available tasks

## IDE Setup

### IntelliJ IDEA

1. Open IntelliJ IDEA
2. Select "Open" and choose the `sdk/java` directory
3. IntelliJ will automatically detect the Gradle project and import it

### Eclipse

1. Install the Gradle plugin (Buildship)
2. File → Import → Gradle → Existing Gradle Project
3. Select the `sdk/java` directory

### VS Code

1. Install the Java Extension Pack
2. Install the Gradle for Java extension
3. Open the `sdk/java` directory

## License

See the main Flezha project for license information.
