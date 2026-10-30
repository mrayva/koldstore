#!/usr/bin/env bash
# Clock skew: the automatic-election test with node wall clocks far apart (A +0 s, B -30 s,
# C +30 s, ttl 8 s). A fast clock must not elect over a live leader, a slow one must not delay a
# real election, and elections must still take about one ttl. See test-auto-election.sh.
#   KOLDSTORE_PKG=/tmp/kl-stage/pkg-fol scripts/test-clock-skew.sh
SKEW_A=${SKEW_A:-0} SKEW_B=${SKEW_B:--30} SKEW_C=${SKEW_C:-30} ELECT_TEST_DIR=${ELECT_TEST_DIR:-/tmp/ks-skew} \
  exec "$(dirname "$0")/test-auto-election.sh"
