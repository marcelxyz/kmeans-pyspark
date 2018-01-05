#!/bin/sh

if [ $# -lt 2 ]
then
    echo "Usage: $0 job_number k"
    exit
fi

USERS=/data/stackoverflow/Users
POSTS=/data/stackoverflow/Posts
POST_HISTORY=/data/stackoverflow/PostHistory
BADGES=/data/stackoverflow/Badges
VOTES=/data/stackoverflow/Votes

SRC_DIR=$(dirname "$0")
LOGS_DIR=${SRC_DIR}/../logs

mkdir -p ${LOGS_DIR}

case "$1" in

1) spark-submit ${SRC_DIR}/index.py user__reputation__to__upvotes_cast $2 $USERS &> ${LOGS_DIR}/user__reputation__to__upvotes_cast
;;

2) spark-submit ${SRC_DIR}/index.py length__aboutme__to__user_rep $2 $USERS &> ${LOGS_DIR}/length__aboutme__to__user_rep
;;

3) spark-submit ${SRC_DIR}/index.py post__edits__average__to__user_rep $2 $USERS $POST_HISTORY &> ${LOGS_DIR}/post__edits__average__to__user_rep
;;

4) spark-submit ${SRC_DIR}/index.py user__membership_time__to__closed_questions $2 $USERS $POSTS $POST_HISTORY &> ${LOGS_DIR}/user__membership_time__to__closed_questions
;;

5) spark-submit ${SRC_DIR}/index.py user__upvotes_cast__to__average_post_length__to__profile_views $2 $USERS $POSTS &> ${LOGS_DIR}/user__upvotes_cast__to__average_post_length__to__profile_views
;;

6) spark-submit ${SRC_DIR}/index.py user__badges__to__signup__to__answers_and_questions $2 $USERS $BADGES $POSTS &> ${LOGS_DIR}/user__badges__to__signup__to__answers_and_questions
;;

7) spark-submit ${SRC_DIR}/index.py user__reputation__to__own_questions_answered $2 $USERS $POSTS &> ${LOGS_DIR}/user__reputation__to__own_questions_answered
;;

8) spark-submit ${SRC_DIR}/index.py user__signup__to__distinct_post_tags $2 $USERS $POSTS &> ${LOGS_DIR}/user__signup__to__distinct_post_tags
;;

9) spark-submit ${SRC_DIR}/index.py user__reputation__to__distinct_post_tags $2 $USERS $POSTS &> ${LOGS_DIR}/user__reputation__to__distinct_post_tags
;;

10) spark-submit ${SRC_DIR}/index.py user_rep_to_answers_and_questions $2 $USERS $POSTS &> ${LOGS_DIR}/user_rep_to_answers_and_questions
;;

11) spark-submit ${SRC_DIR}/index.py user_rep $2 $USERS &> ${LOGS_DIR}/user_rep
;;

12) spark-submit ${SRC_DIR}/index.py user_upvotes_cast $2 $USERS &> ${LOGS_DIR}/user_upvotes_cast
;;

13) spark-submit ${SRC_DIR}/index.py user_downvotes_cast $2 $USERS &> ${LOGS_DIR}/user_downvotes_cast
;;

14) spark-submit ${SRC_DIR}/index.py user_questions_asked $2 $POSTS &> ${LOGS_DIR}/user_questions_asked
;;

15) spark-submit ${SRC_DIR}/index.py user_questions_answered $2 $POSTS &> ${LOGS_DIR}/user_questions_answered
;;

esac