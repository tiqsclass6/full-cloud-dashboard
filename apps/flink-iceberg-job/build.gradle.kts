plugins {
    java
    alias(libs.plugins.shadow)
}

group = "io.lakehouse"
version = "0.1.0"

repositories {
    mavenCentral()
}

// Matches the runtime image (flink:*-java17).
java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(17)
    }
}

// Flink itself is in the runtime image (compile-only); everything else goes into the job jar.
dependencies {
    compileOnly(libs.flink.streaming.java)
    compileOnly(libs.flink.clients)
    compileOnly(libs.flink.table.common)
    compileOnly(libs.flink.table.runtime)
    compileOnly(libs.flink.connector.base)
    compileOnly(libs.slf4j.api)

    implementation(libs.flink.connector.kafka)
    implementation(libs.iceberg.flink.runtime)
    implementation(libs.iceberg.aws.bundle)
    implementation(libs.avro)
    implementation(libs.hadoop.client.api)
    implementation(libs.hadoop.client.runtime)
    implementation(libs.jackson.databind)

    // Security floors for transitive dependencies (Trivy, local/scripts/security-scan.sh).
    constraints {
        implementation("at.yawk.lz4:lz4-java:1.11.4") { because("CVE-2026-106451 (via the Kafka connector)") }
    }

    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.platform.launcher)
    testImplementation(libs.assertj)
    testImplementation(libs.flink.streaming.java)
    testImplementation(libs.flink.clients)
    testImplementation(libs.flink.table.common)
    testImplementation(libs.flink.table.runtime)
    testImplementation(libs.slf4j.api)
    testRuntimeOnly("org.slf4j:slf4j-simple:${libs.versions.slf4j.get()}")
}

tasks.withType<JavaCompile>().configureEach {
    options.encoding = "UTF-8"
    // Flink operators are Serializable; the serial lint would demand serialVersionUID everywhere.
    options.compilerArgs.addAll(listOf("-Xlint:all,-serial", "-Werror"))
}

tasks.test {
    useJUnitPlatform()
    // Flink's Kryo serialization reflects into JDK internals; the Flink image sets the same flags.
    jvmArgs(
        listOf(
            "java.base/java.lang", "java.base/java.lang.invoke", "java.base/java.lang.reflect", "java.base/java.io",
            "java.base/java.net", "java.base/java.nio", "java.base/java.math", "java.base/java.text",
            "java.base/java.time", "java.base/java.util", "java.base/java.util.concurrent",
            "java.base/java.util.concurrent.atomic", "java.base/java.util.concurrent.locks",
        ).map { "--add-opens=$it=ALL-UNNAMED" }
    )
    testLogging {
        events("failed", "skipped")
        exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL
    }
}

tasks.shadowJar {
    archiveFileName = "job.jar"
    isZip64 = true // AWS SDK + Hadoop bundles exceed 65,535 entries
    mergeServiceFiles()
    exclude("META-INF/*.SF", "META-INF/*.DSA", "META-INF/*.RSA")
}

tasks.build {
    dependsOn(tasks.shadowJar)
}
