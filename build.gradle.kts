plugins {
    kotlin("jvm") version "1.9.23"
}

group = "me.mkulak"
version = "1.0"

repositories {
    mavenCentral()
}

dependencies {
    implementation("io.aeron:aeron-all:1.41.6")
    implementation("io.vertx:vertx-web:4.5.4")
    implementation("io.vertx:vertx-web-client:4.5.4")
    implementation("io.vertx:vertx-lang-kotlin-coroutines:4.5.4")

    testImplementation("org.jetbrains.kotlin:kotlin-test")
}

tasks.test {
    useJUnitPlatform()
}
kotlin {
    jvmToolchain(21)
}