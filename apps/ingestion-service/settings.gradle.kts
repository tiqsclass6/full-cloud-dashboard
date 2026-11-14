plugins {
    // Lets Gradle download the JDK requested by the toolchain (Java 21) if it is not installed.
    id("org.gradle.toolchains.foojay-resolver-convention") version "1.0.0"
}

rootProject.name = "ingestion-service"

include("service", "load-generator")
