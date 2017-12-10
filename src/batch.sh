#!/bin/sh

SRC_DIR=$(dirname "$0")

spark-submit ${SRC_DIR}/index.py user__reputation__to__upvotes_cast 3 data/users.xml

spark-submit ${SRC_DIR}/index.py user__reputation__to__upvotes_cast 5 data/users.xml