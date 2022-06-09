/*
 * Copyright (C) 2020 Robert Stupp, All rights reserved.
 * snazy@snazy.de
 */

plugins { `kotlin-dsl` }

repositories {
  mavenCentral()
  gradlePluginPortal()
}

dependencies { implementation(gradleKotlinDsl()) }

kotlinDslPluginOptions { jvmTarget.set(JavaVersion.VERSION_11.toString()) }
