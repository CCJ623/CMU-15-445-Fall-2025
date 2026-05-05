#!/bin/bash
# cmu-15-445-ta/scripts/read_docs.sh
COMMAND=$1
case $COMMAND in
  list) git ls-tree -r docs --name-only ;;
  read) git show docs:"$2" ;;
  search) git grep "$2" docs ;;
  *) echo "Usage: $0 {list|read <path>|search <pattern>}"; exit 1 ;;
esac
