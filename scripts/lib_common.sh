#!/usr/bin/env bash

function app_does_not_need_built {
  local -r app=${1}
  local -r commit_range=${2}

  ! apps_needing_built "${commit_range}" \
    | grep -x "${app}" -q
}

function apps_needing_built {
  local -r commit_range=${1}

  if should_build_all "${commit_range}"; then
    all_apps
  else
    apps_that_have_changed "${commit_range}"
  fi
}

function apps_needing_published {
  local -r commit_range=${1}

  if should_build_all "${commit_range}"; then
    all_publishable_apps
  else
    publishable_apps_that_have_changed "${commit_range}"
  fi
}

function publishable_apps_that_have_changed {
  local -r commit_range=${1}

  comm -12 <(apps_that_have_changed "${commit_range}" | sort) <(all_publishable_apps | sort)
}

function should_build_all {
  local -r commit_range=${1}

  ! git diff --exit-code --quiet ${commit_range} -- mix.lock apps/pipeline apps/dead_letter
}

function apps_that_have_changed {
  local -r commit_range=${1}

  git diff --name-only ${commit_range} -- apps/ \
    | sed 's%apps/%%g' \
    | cut -d/ -f 1 \
    | sort \
    | uniq
}

function all_apps {
  find apps -maxdepth 2 -name mix.exs | awk -F/ '{print $2}'
}

function all_publishable_apps {
  find apps -maxdepth 2 -name Dockerfile | awk -F/ '{print $2}'
}

