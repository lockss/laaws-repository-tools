/*
 * LAAWS Repository Tools
 *
 * Tools for working with the LOCKSS Repository.
 */

plugins {
    id("lockss-java-conventions")
}

group = "org.lockss.laaws"
version = "2.14.0-SNAPSHOT"
description = "LOCKSS Repository Tools"

dependencies {
    // Internal dependencies
    api(project(":lockss-spring-bundle"))

    // Test dependencies
    testImplementation(platform(project(":lockss-pom-bundles:lockss-junit5-bundle")))
    testImplementation(libs.junit.jupiter.engine)
}
