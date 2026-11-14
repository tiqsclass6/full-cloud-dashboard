plugins {
    application
}

group = "io.lakehouse"
version = "0.1.0"

repositories {
    mavenCentral()
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(21)
    }
}

dependencies {
    implementation(libs.iceberg.core)
    implementation(libs.iceberg.aws)             // GlueCatalog and S3FileIO
    runtimeOnly(libs.iceberg.aws.bundle)         // AWS SDK v2 (S3, Glue, STS for IRSA)
    implementation(libs.micrometer.prometheus)
    runtimeOnly(libs.slf4j.simple)

    // Security floors for transitive dependencies (Trivy, local/scripts/security-scan.sh).
    constraints {
        implementation("com.fasterxml.jackson.core:jackson-core:2.22.4") { because("CVE-2026-89407, CVE-2026-89425") }
        implementation("com.fasterxml.jackson.core:jackson-databind:2.22.4") { because("CVE-2026-91776, CVE-2026-91777") }
    }

    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.platform.launcher)
    testImplementation(libs.assertj)
}

application {
    mainClass = "io.lakehouse.exporter.Main"
    applicationName = "iceberg-exporter"
}

tasks.withType<JavaCompile>().configureEach {
    options.encoding = "UTF-8"
    options.compilerArgs.addAll(listOf("-Xlint:all", "-Werror"))
}

tasks.withType<Test>().configureEach {
    useJUnitPlatform()
    testLogging {
        events("failed", "skipped")
        exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL
    }
}
