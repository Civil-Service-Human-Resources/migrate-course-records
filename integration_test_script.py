import random
import string
from datetime import datetime
from typing import Optional, List
from unittest import TestCase

from config import get_mysql_connection, get_pg_connection
from course_completions import CourseCompletion
from learner_record import CourseRecord, get_all_learner_records
from models import CourseRecordPagination
from script import insert_course_records


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


def gen_course_id():
    return f"MIGRATION_COURSE_{gen_id()}"


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
                and user_id like 'MIGRATION_USER_%'
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
    teardown_course_completions()


def generate_course_record(state: Optional[str], preference: Optional[str], last_updated: datetime,
                           module_record_dates: List[datetime]):
    course_record = TestCourseRecord(gen_course_id(), gen_user_id(), state, preference, last_updated)
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

    def test_sync_course_records(self):
        course_record_1 = generate_course_record(None, None, datetime_2024, [])
        course_record_2 = generate_course_record(None, None, datetime_2025, [])
        course_record_3 = generate_course_record(None, None, datetime_2025, [datetime_2024])
        for cr in (course_record_1, course_record_2, course_record_3):
            insert_course_record(cr)
        pagination = CourseRecordPagination(3, 3, 1, 0, 0)
        insert_course_records(pagination, True)
        res = get_all_learner_records()
        res_map = {lr.get_id(): lr for lr in res}
        assert res_map[course_record_1.get_id()].created_timestamp == datetime_2024
        assert res_map[course_record_2.get_id()].created_timestamp == datetime_2025
        assert res_map[course_record_3.get_id()].created_timestamp == datetime_2024
