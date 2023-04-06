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

readonly PROFILES="dataproc21"
readonly MVN="./mvnw -B -e -s /workspace/cloudbuild/gcp-settings.xml -Dmaven.repo.local=/workspace/.repository"

export INDIRECT_WRITE_BUCKET=dataproc-integ-tests
export BIGLAKE_BUCKET=dataproc-integ-tests
export BIGLAKE_CONNECTION=hive-integration-tests

cd /workspace

case "$ACTION" in
  # Download maven and all the dependencies
  build)
    $MVN install -P"${PROFILES}" -DskipTests
    exit
    ;;

  # Run unit tests
  unittest)
    $MVN surefire:test jacoco:report jacoco:report-aggregate -P"${PROFILES}",coverage
    # Upload test coverage report to Codecov
    bash <(curl -s https://codecov.io/bash) -K -F "${ACTION}"
    exit
    ;;

  # Run integration tests
  integrationtest)
    $MVN failsafe:integration-test failsafe:verify jacoco:report jacoco:report-aggregate \
      -P"${PROFILES}",coverage,integration
    # Upload test coverage report to Codecov
    bash <(curl -s https://codecov.io/bash) -K -F "${ACTION}"
    exit
    ;;

  *)
    echo "Unknown action: $ACTION"
    exit 1
    ;;
esac
