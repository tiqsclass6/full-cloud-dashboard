plugins {
    application
}

dependencies {
    implementation(libs.kafka.clients)
    implementation(libs.avro)
    implementation(libs.micrometer.prometheus)
    implementation(libs.jackson.databind)
    runtimeOnly(libs.slf4j.simple)

    // Security floors for transitive dependencies of kafka-clients (Trivy, local/scripts/security-scan.sh).
    constraints {
        implementation("at.yawk.lz4:lz4-java:1.11.4") { because("CVE-2026-106451") }
        implementation("org.xerial.snappy:snappy-java:1.1.10.9") { because("CVE-2026-93451, CVE-2026-108106") }
    }

    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.platform.launcher)
    testImplementation(libs.assertj)
    testImplementation(libs.testcontainers.kafka)
    testImplementation(libs.testcontainers.junit)
}

application {
    mainClass = "io.lakehouse.ingest.Main"
    applicationName = "ingestion-service"
}

// Integration tests (Testcontainers, tag "integration") run with ./gradlew integrationTest.
tasks.test {
    useJUnitPlatform {
        excludeTags("integration")
    }
}

tasks.register<Test>("integrationTest") {
    description = "Runs tests tagged 'integration' (requires Docker)."
    group = "verification"
    testClassesDirs = sourceSets.test.get().output.classesDirs
    classpath = sourceSets.test.get().runtimeClasspath
    useJUnitPlatform {
        includeTags("integration")
    }
    shouldRunAfter(tasks.test)
}
