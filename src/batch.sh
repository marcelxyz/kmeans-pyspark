#!/bin/sh

SRC_DIR=$(dirname "$0")

spark-submit ${SRC_DIR}/index.py user__reputation__to__upvotes_cast 3 /data/stackoverflow/Users
spark-submit ${SRC_DIR}/index.py user__reputation__to__upvotes_cast 4 /data/stackoverflow/Users
spark-submit ${SRC_DIR}/index.py user__reputation__to__upvotes_cast 5 /data/stackoverflow/Users

spark-submit ${SRC_DIR}/index.py length__aboutme__to__user_rep 3 /data/stackoverflow/Users
spark-submit ${SRC_DIR}/index.py length__aboutme__to__user_rep 4 /data/stackoverflow/Users
spark-submit ${SRC_DIR}/index.py length__aboutme__to__user_rep 5 /data/stackoverflow/Users

spark-submit ${SRC_DIR}/index.py post__edits__average__to__user_rep 3 /data/stackoverflow/Users /data/stackoverflow/PostHistory
spark-submit ${SRC_DIR}/index.py post__edits__average__to__user_rep 4 /data/stackoverflow/Users /data/stackoverflow/PostHistory
spark-submit ${SRC_DIR}/index.py post__edits__average__to__user_rep 5 /data/stackoverflow/Users /data/stackoverflow/PostHistory

spark-submit ${SRC_DIR}/index.py user__membership_time__to__closed_questions 3 /data/stackoverflow/Users /data/stackoverflow/Posts /data/stackoverflow/PostHistory
spark-submit ${SRC_DIR}/index.py user__membership_time__to__closed_questions 4 /data/stackoverflow/Users /data/stackoverflow/Posts /data/stackoverflow/PostHistory
spark-submit ${SRC_DIR}/index.py user__membership_time__to__closed_questions 5 /data/stackoverflow/Users /data/stackoverflow/Posts /data/stackoverflow/PostHistory

spark-submit ${SRC_DIR}/index.py user__upvotes_cast__to__average_post_length__to__profile_views 3 /data/stackoverflow/Users /data/stackoverflow/Posts
spark-submit ${SRC_DIR}/index.py user__upvotes_cast__to__average_post_length__to__profile_views 4 /data/stackoverflow/Users /data/stackoverflow/Posts
spark-submit ${SRC_DIR}/index.py user__upvotes_cast__to__average_post_length__to__profile_views 5 /data/stackoverflow/Users /data/stackoverflow/Posts