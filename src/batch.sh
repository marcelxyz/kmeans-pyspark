#!/bin/sh

SRC_DIR=$(dirname "$0")
USERS=/data/stackoverflow/Users
POSTS=/data/stackoverflow/Posts
POST_HISTORY=/data/stackoverflow/PostHistory
BADGES=/data/stackoverflow/Badges
VOTES=/data/stackoverflow/Votes

spark-submit ${SRC_DIR}/index.py user__reputation__to__upvotes_cast 4 $USERS

spark-submit ${SRC_DIR}/index.py length__aboutme__to__user_rep 4 $USERS

spark-submit ${SRC_DIR}/index.py post__edits__average__to__user_rep 4 $USERS $POST_HISTORY

spark-submit ${SRC_DIR}/index.py user__membership_time__to__closed_questions 4 $USERS $POSTS $POST_HISTORY

spark-submit ${SRC_DIR}/index.py user__upvotes_cast__to__average_post_length__to__profile_views 4 $USERS $POSTS

spark-submit ${SRC_DIR}/index.py user__badges__to__signup__to__answers_and_questions 4 $USERS $BADGES $POSTS

spark-submit ${SRC_DIR}/index.py user__reputation__to__own_questions_answered 4 $USERS $POSTS

spark-submit ${SRC_DIR}/index.py user__signup__to__distinct_post_tags 4 $USERS $POSTS

spark-submit ${SRC_DIR}/index.py user__reputation__to__distinct_post_tags 4 $USERS $POSTS

spark-submit ${SRC_DIR}/index.py user_rep_to_answers_and_questions 4 $USERS $POSTS

spark-submit ${SRC_DIR}/index.py user_rep_to_bounty 4 $USERS $VOTES $POSTS