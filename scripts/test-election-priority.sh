#!/usr/bin/env bash
# Best election candidate: with koldstore.election_priority set per node, the automatic election
# must pick the highest-priority caught-up follower, fall back down the order when it dies, and
# never pick a node with priority 0. Runs the automatic-election test twice. See that script.
#   KOLDSTORE_PKG=/tmp/kl-stage/pkg-fol scripts/test-election-priority.sh
here="$(dirname "$0")"
echo "##### priorities A=100 B=50 C=200: C first, then A"
PRIO_A=100 PRIO_B=50 PRIO_C=200 EXPECT_FIRST=c EXPECT_SECOND=a ELECT_TEST_DIR=/tmp/ks-prio1 "$here/test-auto-election.sh" || exit 1
echo "##### priorities A=10 B=100 C=0 (C opted out): B first, then A"
PRIO_A=10 PRIO_B=100 PRIO_C=0 EXPECT_FIRST=b EXPECT_SECOND=a ELECT_TEST_DIR=/tmp/ks-prio2 "$here/test-auto-election.sh"
