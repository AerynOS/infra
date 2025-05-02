#!/usr/bin/env bash
#
set -x

if [[ -d "summit/state/cache/repository" ]]
then
  for r in summit/state/cache/repository/*/clone/
  do
      # Avoid dying if dirs do not exist
      [[ -d "${r}" ]] && git -C "${r}" remote update
  done
fi

set +x

