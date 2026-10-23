#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Ostrium Labs fork only. Disables every workflow in the fork except the
# fork's own `loams-*.yml` ones. Run it after every upstream sync: a workflow
# file that upstream adds starts out active. Needs `gh` with admin rights on
# the fork.

set -euo pipefail

repo="${1:-ostrium-labs/iggy}"

gh api "repos/$repo/actions/workflows?per_page=100" \
  --jq '.workflows[] | select(.state == "active") | "\(.id)\t\(.path)"' |
  while IFS=$'\t' read -r id path; do
    case "$path" in
      .github/workflows/loams-*) echo "keep    $path" ;;
      *)
        gh workflow disable "$id" -R "$repo"
        echo "disable $path"
        ;;
    esac
  done
