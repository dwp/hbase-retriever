import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    java
    kotlin("jvm") version "1.3.41"
}

group = "uk.org.dwp.dataworks"
version = "0.0.0"

repositories {
    mavenCentral()
    jcenter()
    maven(url = "https://jitpack.io")
}

dependencies {
    implementation(kotlin("stdlib-jdk8"))
    implementation("com.amazonaws", "aws-lambda-java-core", "1.2.0")
    implementation("org.apache.hbase", "hbase-client", "1.5.0")
    implementation("com.beust", "klaxon", "4.0.2")
    implementation("junit:junit:4.12")
    implementation("org.jetbrains.kotlin:kotlin-reflect:1.3.0")
    implementation("com.github.dwp:dataworks-common-logging:0.0.5")
    implementation("ch.qos.logback", "logback-classic", "1.2.3")
    implementation("ch.qos.logback", "logback-core", "1.2.3")
    testImplementation("io.kotlintest", "kotlintest-runner-junit5", "3.3.2")
    testImplementation("com.nhaarman.mockitokotlin2", "mockito-kotlin", "2.2.0")
}

configurations.all {
    exclude(group="org.slf4j", module="slf4j-log4j12")
}

tasks.withType<KotlinCompile> {
    kotlinOptions.jvmTarget = "1.8"
}

tasks.named<Jar>("jar") {
    archiveClassifier.set("uber")

    from(sourceSets.main.get().output)

    dependsOn(configurations.runtimeClasspath)
    from({
        configurations.runtimeClasspath.get().filter { it.name.endsWith("jar") }.map { zipTree(it) }
    })
}
