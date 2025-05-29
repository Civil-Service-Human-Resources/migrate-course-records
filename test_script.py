import datetime
from copy import copy

from course_completions import CourseCompletion
from learner_record import LearnerRecordWithEvents, CourseRecord
from script import find_course_completion_events, find_non_completion_events

created = datetime.datetime.now()

learner_records = {
    "course_1,user_1": LearnerRecordWithEvents("course_1", "user_1", 1, created),
    "course_2,user_1": LearnerRecordWithEvents("course_2", "user_1", 2, created),
    "course_3,user_2": LearnerRecordWithEvents("course_3", "user_2", 3, created)
}


def test_find_events():
    completions = [
        CourseCompletion("course_1", "user_1", created),
        CourseCompletion("course_1", "user_1", created),
        CourseCompletion("course_2", "user_1", created)
    ]
    result = find_course_completion_events(copy(learner_records), completions)
    assert len(result["course_1,user_1"].events) == 2
    assert result["course_1,user_1"].has_completions == True

    assert len(result["course_2,user_1"].events) == 1
    assert result["course_2,user_1"].has_completions == True

    assert len(result["course_3,user_2"].events) == 0
    assert result["course_3,user_2"].has_completions == False


def test_find_non_completion_events():
    records = copy(learner_records)
    course_records = [
        CourseRecord("course_1", "user_1", "ARCHIVED", None, created, created),
        CourseRecord("course_2", "user_1", None, "LIKED", created, created),
        CourseRecord("course1", "user_1", "IN_PROGRESS", None, created, created),
    ]

    result = find_non_completion_events(records, course_records)
    assert len(result["course_1,user_1"].events) == 1
    assert result["course_1,user_1"].events[0].event_id == 2
    assert len(result["course_2,user_1"].events) == 1
    assert result["course_1,user_1"].events[0].event_id == 1
    assert len(result["course_3,user_2"].events) == 0
