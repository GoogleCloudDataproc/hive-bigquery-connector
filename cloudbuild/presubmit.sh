#!/bin/bash

# Copyright 2022 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the 'License');
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#            http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -euxo pipefail

if [ -z "${CODECOV_TOKEN}" ]; then
  echo "missing environment variable CODECOV_TOKEN"
  exit 1
fi


readonly ACTION=$1

readonly HIVE1_PROFILE="hive1-generic"
readonly HIVE2_PROFILE="hive2-generic"
readonly HIVE3_PROFILE="hive3-generic"
readonly HIVE2_SHADED_DEPS="shaded-deps-hive2.3.9-hadoop2.10.2"
readonly HIVE3_SHADED_DEPS="shaded-deps-hive3.1.2-hadoop2.10.2"
readonly MVN="./mvnw -B -e -Dmaven.repo.local=/workspace/.repository"

export TEST_BUCKET=dataproc-integ-tests
export BIGLAKE_BUCKET=dataproc-integ-tests
export BIGLAKE_CONNECTION=hive-integration-tests

cd /workspace

case "$ACTION" in
  # Java code style check
  check)
    $MVN spotless:check -P"${HIVE1_PROFILE}" && $MVN spotless:check -P"${HIVE2_PROFILE}" && $MVN spotless:check -P"${HIVE3_PROFILE}"
    exit
    ;;

  # Build the Maven packages and dependencies
  build)
    # Install all modules for Hive 1
    $MVN install -DskipTests -P"${HIVE1_PROFILE}"
    # Install the shaded dependencies for Hive 2 (all the other shaded & parent modules have already been installed with the previous command)
    $MVN install -DskipTests -P"${HIVE2_PROFILE}" -pl ${HIVE2_SHADED_DEPS}
    # Install the shaded dependencies for Hive 3 (all the other shaded & parent modules have already been installed with the previous command)
    $MVN install -DskipTests -P"${HIVE3_PROFILE}" -pl ${HIVE3_SHADED_DEPS}
    exit
    ;;

  # Run unit tests for Hive 1.x.x
  unittest_hive1)
    $MVN surefire:test jacoco:report jacoco:report-aggregate -P"${HIVE1_PROFILE}",coverage
    # Upload test coverage report to Codecov
    bash <(curl -s https://codecov.io/bash) -K -F "${ACTION}"
    exit
    ;;

  # Run unit tests for Hive 2.x.x
  unittest_hive2)
    $MVN surefire:test jacoco:report jacoco:report-aggregate -P"${HIVE2_PROFILE}",coverage
    # Upload test coverage report to Codecov
    bash <(curl -s https://codecov.io/bash) -K -F "${ACTION}"
    exit
    ;;

  # Run unit tests for Hive 3.x.x
  unittest_hive3)
    $MVN surefire:test jacoco:report jacoco:report-aggregate -P"${HIVE3_PROFILE}",coverage
    # Upload test coverage report to Codecov
    bash <(curl -s https://codecov.io/bash) -K -F "${ACTION}"
    exit
    ;;

  # Run integration tests for Hive 1.x.x
  integrationtest_hive1)
    $MVN failsafe:integration-test failsafe:verify jacoco:report jacoco:report-aggregate \
      -P"${HIVE1_PROFILE}",coverage,integration
    # Upload test coverage report to Codecov
    bash <(curl -s https://codecov.io/bash) -K -F "${ACTION}"
    exit
    ;;

  # Run integration tests for Hive 2.x.x
  integrationtest_hive2)
    $MVN failsafe:integration-test failsafe:verify jacoco:report jacoco:report-aggregate \
      -P"${HIVE2_PROFILE}",coverage,integration
    # Upload test coverage report to Codecov
    bash <(curl -s https://codecov.io/bash) -K -F "${ACTION}"
    exit
    ;;

  # Run integration tests for Hive 3.x.x
  integrationtest_hive3)
    $MVN failsafe:integration-test failsafe:verify jacoco:report jacoco:report-aggregate \
      -P"${HIVE3_PROFILE}",coverage,integration
    # Upload test coverage report to Codecov
    bash <(curl -s https://codecov.io/bash) -K -F "${ACTION}"
    exit
    ;;

  *)
    echo "Unknown action: $ACTION"
    exit 1
    ;;
esac
