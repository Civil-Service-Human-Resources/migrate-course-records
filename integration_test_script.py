import random
import string
from datetime import datetime
from typing import Optional, List
from unittest import TestCase

from config import get_mysql_connection, get_pg_connection
from course_completions import CourseCompletion
from learner_record import CourseRecord, get_all_learner_records
from script import insert_course_records_for_missing_users, get_missing_user_ids_to_fetch, fetch_all_lr_map, \
    apply_course_completion_events, apply_non_completion_events


class ModuleRecord:
    def __init__(self, module_id: str, created_at: datetime, course_id: str, user_id: str):
        self.module_id = module_id
        self.created_at = created_at
        self.course_id = course_id
        self.user_id = user_id


class TestCourseRecord(CourseRecord):
    def __init__(self, course_id, user_id, state: Optional[str], preference: Optional[str], last_updated: datetime,
                 module_records: Optional[List[ModuleRecord]] = None):
        super().__init__(course_id, user_id, state, preference, last_updated)
        if not module_records:
            module_records = []
        self.module_records = module_records


def gen_course_id(_id):
    return f"MIGRATION_COURSE_{_id}"


def gen_user_id():
    return f"MIGRATION_USER_{gen_id()}"


def gen_id():
    return ''.join(random.choices(string.ascii_letters + string.digits, k=10))


def insert_course_record(course_record: TestCourseRecord):
    conn = get_mysql_connection()
    with conn.cursor() as cursor:
        sql = """
              INSERT INTO course_record
                  (course_id, user_id, state, preference, last_updated, course_title)
              VALUES (%s, %s, %s, %s, %s, %s)
              """

        vals = (
            course_record.course_id,
            course_record.user_id,
            course_record.state,
            course_record.preference,
            course_record.last_updated,
            f"TEST_COURSE_{course_record.course_id}"
        )

        cursor.execute(sql, vals)

    conn.commit()
    for module_record in course_record.module_records:
        insert_module_record(module_record)


def insert_course_completion(course_completion: CourseCompletion):
    conn = get_pg_connection()
    with conn.cursor() as cursor:
        sql = """
              INSERT INTO public.course_completion_events (external_id, user_id, course_id, course_title,
                                                           event_timestamp, organisation_id, profession_id)
              VALUES (%s, %s, %s, %s, %s, %s, %s)
              """

        vals = (
            f"MIGRATE_{gen_id()}",
            course_completion.user_id,
            course_completion.course_id,
            f"TEST_COURSE_{course_completion.course_id}",
            course_completion.event_timestamp,
            1,
            1
        )
        cursor.execute(sql, vals)
    conn.commit()


def insert_module_record(module_record: ModuleRecord):
    conn = get_mysql_connection()
    with conn.cursor() as cursor:
        sql = """
              INSERT INTO module_record (module_id, created_at, course_id, user_id)
              VALUES (%s, %s, %s, %s)
              """

        vals = (
            module_record.module_id,
            module_record.created_at,
            module_record.course_id,
            module_record.user_id
        )

        cursor.execute(sql, vals)
    conn.commit()


def teardown_course_records():
    conn = get_mysql_connection()
    with conn.cursor() as cursor:
        sql = """
              DELETE
              FROM course_record
              WHERE course_id like 'MIGRATION_COURSE_%'
              """
        cursor.execute(sql)
    conn.commit()


def teardown_learner_records():
    conn = get_mysql_connection()
    with conn.cursor() as cursor:
        sql = """
              DELETE
              FROM learner_records
              WHERE resource_id like 'MIGRATION_COURSE_%'
              """
        cursor.execute(sql)
    conn.commit()


def teardown_course_completions():
    conn = get_pg_connection()
    with conn.cursor() as cursor:
        sql = """
              DELETE
              FROM course_completion_events
              WHERE external_id like 'MIGRATE_%'
              """
        cursor.execute(sql)
    conn.commit()


def teardown():
    teardown_course_records()
    teardown_learner_records()
    teardown_course_completions()


def generate_course_record(_id: str, state: Optional[str], preference: Optional[str], last_updated: datetime,
                           module_record_dates: List[datetime]):
    course_record = TestCourseRecord(gen_course_id(_id), gen_user_id(), state, preference, last_updated)
    for date in module_record_dates:
        generate_module_record(course_record, date)
    return course_record


def generate_module_record(course_record: TestCourseRecord, created_at: datetime):
    module_record = ModuleRecord(gen_id(), created_at, course_record.course_id, course_record.user_id)
    course_record.module_records.append(module_record)


def generate_course_completion(course_record: TestCourseRecord, timestamp: datetime):
    return CourseCompletion(course_record.course_id, course_record.user_id, timestamp)


datetime_2025 = datetime(2025, 1, 1, 10, 0, 0)
datetime_2024 = datetime(2024, 1, 1, 10, 0, 0)


class IntegrationTests(TestCase):

    def tearDown(self):
        teardown()

    def setUp(self):
        teardown()

    def test_run(self):
        # Ambiguous; record is in progress and liked so no clear events. Just the record should be created
        course_record_1 = generate_course_record('1', 'IN_PROGRESS', 'LIKED', datetime_2024, [])

        # Moved to learning plan; record is NULL state and liked. Record + 1 MOVE_TO_LEARNING_PLAN event
        course_record_2 = generate_course_record('2', None, 'LIKED', datetime_2025, [])

        # Removed from suggestions; record is NULL state and disliked. Record + 1 REMOVE_FROM_SUGGESTIONS event
        course_record_3 = generate_course_record('3', None, 'DISLIKED', datetime_2025, [])

        # Completed; record is COMPLETED and has a course completion. Record + 1 COURSE_COMPLETE event
        course_record_4 = generate_course_record('4', 'COMPLETED', None, datetime_2025, [datetime_2024])
        course_record_4_completion = generate_course_completion(course_record_4, datetime_2024)
        insert_course_completion(course_record_4_completion)

        # Completed; record is COMPLETED and has two course completions. Record + 1 COURSE_COMPLETE event
        course_record_5 = generate_course_record('5', 'COMPLETED', None, datetime_2024, [])
        course_record_5_completion = generate_course_completion(course_record_5, datetime_2024)
        course_record_5_completion_2 = generate_course_completion(course_record_5, datetime_2025)
        [insert_course_completion(completion) for completion in
         (course_record_5_completion, course_record_5_completion_2)]

        # Removed from learning plan; record is ARCHIVED. Record + 1 REMOVE_FROM_LEARNING_PLAN event
        course_record_6 = generate_course_record('6', 'ARCHIVED', 'LIKED', datetime_2025, [datetime_2024])

        # Only obtainable action is remove from learning plan; record is ARCHIVED but the created timestamp matches the last_updated timestamp
        # Just the record should be created because remove from learning plan cannot be the first action taken
        course_record_7 = generate_course_record('7', 'ARCHIVED', 'LIKED', datetime_2024, [datetime_2024])
        for cr in (course_record_1, course_record_2, course_record_3, course_record_4, course_record_5, course_record_6,
                   course_record_7):
            insert_course_record(cr)

        missing_ids = [_id for _id in get_missing_user_ids_to_fetch() if _id.startswith('MIGRATION_')]
        assert len(missing_ids) == 7

        insert_course_records_for_missing_users(missing_ids, True)
        res = get_all_learner_records()
        res_map = {lr.get_id(): lr for lr in res}
        assert res_map[course_record_1.get_id()].created_timestamp == datetime_2024
        assert res_map[course_record_2.get_id()].created_timestamp == datetime_2025
        assert res_map[course_record_3.get_id()].created_timestamp == datetime_2025
        assert res_map[course_record_4.get_id()].created_timestamp == datetime_2024
        assert res_map[course_record_5.get_id()].created_timestamp == datetime_2024
        assert res_map[course_record_6.get_id()].created_timestamp == datetime_2024
        assert res_map[course_record_7.get_id()].created_timestamp == datetime_2024

        _map = fetch_all_lr_map()
        _map = apply_course_completion_events(_map)
        _map = apply_non_completion_events(_map)

        assert len(_map[course_record_1.get_id()].events) == 0

        assert len(_map[course_record_2.get_id()].events) == 1
        assert _map[course_record_2.get_id()].events[0].event_id == 1

        assert len(_map[course_record_3.get_id()].events) == 1
        assert _map[course_record_3.get_id()].events[0].event_id == 3

        assert len(_map[course_record_4.get_id()].events) == 1
        assert _map[course_record_4.get_id()].events[0].event_id == 4

        assert len(_map[course_record_5.get_id()].events) == 2
        assert _map[course_record_5.get_id()].events[0].event_id == 4
        assert _map[course_record_5.get_id()].events[1].event_id == 4

        assert len(_map[course_record_6.get_id()].events) == 1
        assert _map[course_record_6.get_id()].events[0].event_id == 2

        assert len(_map[course_record_7.get_id()].events) == 0
