plugins {
    application
}

dependencies {
    implementation(libs.jackson.databind)
    implementation(libs.hdrhistogram)
    runtimeOnly(libs.slf4j.simple)

    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.platform.launcher)
    testImplementation(libs.assertj)
}

application {
    mainClass = "io.lakehouse.loadgen.LoadGen"
    applicationName = "loadgen"
}
