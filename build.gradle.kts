import org.gradle.api.tasks.testing.logging.TestExceptionFormat
import org.gradle.api.tasks.testing.logging.TestLogEvent
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
    implementation("com.github.dwp:dataworks-common-logging:0.0.4")
    testImplementation("io.kotlintest", "kotlintest-runner-junit5", "3.3.2")
    runtimeOnly("ch.qos.logback:logback-classic:1.2.3")
    runtimeOnly("ch.qos.logback:logback-core:1.2.3")
}

tasks.withType<Test> {
    useJUnitPlatform { }
}

tasks.withType<KotlinCompile> {
    kotlinOptions.jvmTarget = "1.8"
}

sourceSets {
    create("unit") {
        java.srcDir(file("src/test/kotlin"))
        compileClasspath += sourceSets.getByName("main").output + configurations.testRuntimeClasspath
        runtimeClasspath += output + compileClasspath
    }
}

tasks.named<Jar>("jar") {
    archiveClassifier.set("uber")

    from(sourceSets.main.get().output)

    dependsOn(configurations.runtimeClasspath)
    from({
        configurations.runtimeClasspath.get().filter { it.name.endsWith("jar") }.map { zipTree(it) }
    })
}

tasks.register<Test>("unit") {
    description = "Runs the unit tests"
    group = "verification"
    testClassesDirs = sourceSets["unit"].output.classesDirs
    classpath = sourceSets["unit"].runtimeClasspath

    useJUnitPlatform { }
    testLogging {
        exceptionFormat = TestExceptionFormat.FULL
        events = setOf(TestLogEvent.SKIPPED, TestLogEvent.PASSED, TestLogEvent.FAILED)
    }
}
