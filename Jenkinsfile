/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

pipeline {
  agent { label 'local' }

  options {
    disableConcurrentBuilds()
    skipDefaultCheckout(true)
    timestamps()
    buildDiscarder(logRotator(numToKeepStr: '20'))
  }

  parameters {
    string(name: 'BRANCH_TO_BUILD', defaultValue: 'release-1.2.0', description: 'Git branch to build.')
    booleanParam(name: 'FORCE_UPDATE', defaultValue: true, description: 'Pass -U to Maven to refresh snapshot and cached dependency resolution.')
  }

  environment {
    DOCKER_IMAGE = 'maven:3.9.9-eclipse-temurin-17'
    HOST_MAVEN_REPO = '/home/jenkinsmaster/.m2'
    MAVEN_LOCAL_REPO = '/maven-repo/repository'
    MAVEN_OPTS = '-Xmx4G'
    MAVEN_SETTINGS = "${WORKSPACE}@tmp/mvn-settings.xml"
    DEPLOY_REPOSITORY = 'HopsEE::default::https://nexus.hops.works/repository/hudi'
    DEPLOY_REPOSITORY_2 = 'HopsEE::default::https://nexus.hops.works/repository/hops-artifacts'
    HUDI_REPOSITORY = '/opt/repository/master/hudi'
  }

  stages {
    stage('Checkout') {
      steps {
        deleteDir()
        checkout([$class: 'GitSCM',
          branches: [[name: "${params.BRANCH_TO_BUILD}"]],
          userRemoteConfigs: [[
            url: 'git@github.com:gibchikafa/hudi.git',
            credentialsId: 'id_rsa'
          ]]
        ])
      }
    }

    stage('Prepare Maven') {
      steps {
        withCredentials([usernamePassword(credentialsId: 'a0770738-4ef3-4acc-a6ba-097ee6c85b44', passwordVariable: 'PASSWORD', usernameVariable: 'USERNAME')]) {
          sh '''#!/bin/bash -eu
            rm -rf "$WORKSPACE/.m2" "$HOST_MAVEN_REPO/repository/io/hops/hudi"
            mkdir -p "$(dirname "$MAVEN_SETTINGS")" "$HOST_MAVEN_REPO/repository"
            cat > "$MAVEN_SETTINGS" <<EOF
<settings>
  <localRepository>${MAVEN_LOCAL_REPO}</localRepository>
  <servers>
    <server>
      <id>HopsEE</id>
      <username>$USERNAME</username>
      <password>$PASSWORD</password>
    </server>
    <server>
      <id>Hops</id>
      <username>$USERNAME</username>
      <password>$PASSWORD</password>
    </server>
    <server>
      <id>HopsHive</id>
      <username>$USERNAME</username>
      <password>$PASSWORD</password>
    </server>
  </servers>
</settings>
EOF
          '''
        }
      }
    }

    stage('Resolve Version') {
      steps {
        sh '''#!/bin/bash -eu
          perl -0ne 'if (m{<groupId>io\\.hops\\.hudi</groupId>\\s*<artifactId>hudi</artifactId>\\s*<packaging>pom</packaging>\\s*<version>([^<]+)</version>}) { print "$1"; exit }' pom.xml > version.log
          echo "POM_VERSION=$(cat version.log)"
        '''
      }
    }

    stage('Build and Deploy') {
      steps {
        sh '''#!/bin/bash -eu
          UPDATE_ARG=""
          if [ "$FORCE_UPDATE" = "true" ]; then
            UPDATE_ARG="-U"
          fi

          docker run --rm \
            -u "$(id -u):$(id -g)" \
            -v "$WORKSPACE:$WORKSPACE" \
            -v "$(dirname "$MAVEN_SETTINGS"):$(dirname "$MAVEN_SETTINGS")" \
            -v "$HOST_MAVEN_REPO:/maven-repo" \
            -w "$WORKSPACE" \
            -e HOME=/tmp \
            -e MAVEN_CONFIG=/tmp/maven-config \
            -e MAVEN_LOCAL_REPO="$MAVEN_LOCAL_REPO" \
            -e MAVEN_OPTS="$MAVEN_OPTS" \
            -e MAVEN_SETTINGS="$MAVEN_SETTINGS" \
            -e DEPLOY_REPOSITORY="$DEPLOY_REPOSITORY" \
            -e DEPLOY_REPOSITORY_2="$DEPLOY_REPOSITORY_2" \
            -e UPDATE_ARG="$UPDATE_ARG" \
            "$DOCKER_IMAGE" \
            bash -lc '
              set -eu
              export PATH="$JAVA_HOME/bin:$PATH"
              JAVA_VERSION="$("$JAVA_HOME/bin/java" -XshowSettings:properties -version 2>&1 | awk -F"= " "/java.specification.version =/{print \\$2; exit}")"
              if [ "$JAVA_VERSION" != "17" ]; then
                echo "Java 17 is required, but JAVA_HOME=$JAVA_HOME reports java.specification.version=$JAVA_VERSION" >&2
                exit 1
              fi
              test -x "$JAVA_HOME/bin/javadoc"
              mvn -s "$MAVEN_SETTINGS" -Dmaven.repo.local="$MAVEN_LOCAL_REPO" $UPDATE_ARG \
                clean deploy -DskipTests -DaltDeploymentRepository="$DEPLOY_REPOSITORY"
              mvn -s "$MAVEN_SETTINGS" -Dmaven.repo.local="$MAVEN_LOCAL_REPO" \
                deploy -DskipTests -DaltDeploymentRepository="$DEPLOY_REPOSITORY_2"
            '
        '''
      }
    }

    stage('Publish Bundle Jars') {
      steps {
        sh '''#!/bin/bash -eu
          HUDI_VERSION="$(cat version.log)"
          TARGET_DIR="$HUDI_REPOSITORY/$HUDI_VERSION"

          mkdir -p "$TARGET_DIR"
          cp "packaging/hudi-hadoop-mr-bundle/target/hudi-hadoop-mr-bundle-${HUDI_VERSION}.jar" "$TARGET_DIR/"
          cp "packaging/hudi-spark-bundle/target/hudi-spark4.1-bundle_2.13-${HUDI_VERSION}.jar" "$TARGET_DIR/"
          cp "packaging/hudi-utilities-slim-bundle/target/hudi-utilities-slim-bundle_2.13-${HUDI_VERSION}.jar" "$TARGET_DIR/"

          ls -l \
            "$TARGET_DIR/hudi-hadoop-mr-bundle-${HUDI_VERSION}.jar" \
            "$TARGET_DIR/hudi-spark4.1-bundle_2.13-${HUDI_VERSION}.jar" \
            "$TARGET_DIR/hudi-utilities-slim-bundle_2.13-${HUDI_VERSION}.jar"
        '''
      }
    }
  }

  post {
    always {
      sh '''#!/bin/bash
        rm -f "$MAVEN_SETTINGS"
      '''
      archiveArtifacts artifacts: 'version.log', allowEmptyArchive: true
    }
  }
}
