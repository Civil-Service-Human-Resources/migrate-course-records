from datetime import datetime
from typing import List, Optional, Set

from config import get_mysql_connection, event_source_id, course_record_page_size
from log import get_logger
from models import CourseRecordBase

logger = get_logger('learner_record')


class LearnerRecord(CourseRecordBase):
    def __init__(self, course_id, user_id, lr_id: int, created_timestamp: datetime):
        super().__init__(course_id, user_id)
        self.lr_id = lr_id
        self.created_timestamp = created_timestamp


class LearnerRecordEvent:
    def __init__(self, learner_record_id, event_id: int, event_timestamp: datetime):
        self.learner_record_id = learner_record_id
        self.event_id = event_id
        self.event_timestamp = event_timestamp


class LearnerRecordWithEvents(LearnerRecord):
    def __init__(self, course_id, user_id, lr_id: int, created_timestamp: datetime,
                 events: List[LearnerRecordEvent] = None, has_completions: bool = False):
        super().__init__(course_id, user_id, lr_id, created_timestamp)
        if events is None:
            events = []
        self.events = events
        self.has_completions = has_completions

    def sort_events(self):
        self.events.sort(key=lambda x: x.event_timestamp)


class BasicCourseRecord(CourseRecordBase):
    def __init__(self, course_id, user_id, created_at: Optional[datetime], last_updated: datetime):
        super().__init__(course_id, user_id)
        self.created_at = created_at
        self.last_updated = last_updated

    def get_created_timestamp(self):
        return self.created_at if self.created_at is not None else self.last_updated


class CourseRecord(BasicCourseRecord):
    def __init__(self, course_id, user_id, state: Optional[str], preference: Optional[str],
                 created_at: Optional[datetime],
                 last_updated: datetime):
        super().__init__(course_id, user_id, created_at, last_updated)
        self.state = state
        self.preference = preference


class CombinedRecord(CourseRecordBase):
    def __init__(self, course_id: str, user_id: str, learner_record_with_events: LearnerRecordWithEvents,
                 course_record: CourseRecord):
        super().__init__(course_id, user_id)
        self.learner_record_with_events = learner_record_with_events
        self.course_record = course_record


def insert_learner_records(learner_records: List[LearnerRecord]):
    logger.info(f"Inserting {len(learner_records)} total records")
    connection = get_mysql_connection()
    for _i in range(0, len(learner_records), 1000):
        batch = learner_records[_i:_i + 1000]
        logger.info(f"Inserting {len(batch)} records")
        values = []
        for row in batch:
            values.append(f"(1, UUID(), '{row.user_id}', '{row.course_id}', '{row.created_timestamp}')")
        value_sql = ",".join(values)
        sql = f"""
              INSERT IGNORE INTO learner_records (learner_record_type, learner_record_uid, learner_id, resource_id, created_timestamp)
              VALUES {value_sql};
              """
        with connection.cursor() as cursor:
            cursor.execute(sql)
        connection.commit()


def delete_learner_records():
    logger.info("Tearing down learner records")
    connection = get_mysql_connection()
    sql = """
          delete
          from learner_records; \
          """
    with connection.cursor() as cursor:
        cursor.execute(sql)
    connection.commit()


def delete_learner_record_events():
    logger.info("Tearing down learner record events")
    connection = get_mysql_connection()
    sql = """
          delete
          from learner_record_events; \
          """
    with connection.cursor() as cursor:
        cursor.execute(sql)
    connection.commit()


def insert_learner_record_events(learner_record_events: List[LearnerRecordEvent]):
    logger.info(f"Inserting {len(learner_record_events)} total events")
    connection = get_mysql_connection()
    for _i in range(0, len(learner_record_events), 1000):
        batch = learner_record_events[_i:_i + 1000]
        logger.info(f"Inserting {len(batch)} events")
        values = []
        for row in batch:
            values.append(f"({row.learner_record_id}, {row.event_id}, {event_source_id}, '{row.event_timestamp}')")
        value_sql = ",".join(values)
        sql = f"""
                INSERT INTO learner_record_events (learner_record_id, learner_record_event_type, learner_record_event_source, event_timestamp)
                VALUES {value_sql};
              """
        with connection.cursor() as cursor:
            cursor.execute(sql)
        connection.commit()


def get_all_learner_records():
    logger.info("Fetching all learner records")
    conn = get_mysql_connection()
    with conn.cursor() as cursor:
        sql = f"""
            SELECT lr.resource_id as 'course_id', lr.learner_id as 'user_id', lr.id, lr.created_timestamp
            FROM learner_records lr;
        """
        cursor.execute(sql)
        return [LearnerRecordWithEvents(row[0], row[1], row[2], row[3]) for row in cursor.fetchall()]


def count_learner_records():
    conn = get_mysql_connection()
    with conn.cursor() as cursor:
        sql = f"""
            select count(*) from learner_records
        """
        cursor.execute(sql)
        return int(cursor.fetchone()[0])


# Events

MOVE_TO_LEARNING_PLAN = 1
REMOVE_FROM_LEARNING_PLAN = 2
REMOVE_FROM_SUGGESTIONS = 3
COMPLETE_COURSE = 4


def count_course_records():
    conn = get_mysql_connection()
    with conn.cursor() as cursor:
        sql = f"""
            select count(*) from course_record
        """
        cursor.execute(sql)
        return int(cursor.fetchone()[0])


def count_non_completed_course_records():
    conn = get_mysql_connection()
    with conn.cursor() as cursor:
        sql = f"""
            select count(*) from course_record where state != 'COMPLETED'
        """
        cursor.execute(sql)
        return int(cursor.fetchone()[0])


def get_course_records(page):
    offset = (page - 1) * course_record_page_size
    conn = get_mysql_connection()
    with conn.cursor() as cursor:
        sql = f"""
            SELECT cr.course_id, cr.user_id, MIN(mr.created_at) AS 'created_at', cr.last_updated
            from learner_record.course_record cr
            LEFT OUTER JOIN learner_record.module_record mr ON mr.course_id = cr.course_id AND mr.user_id = cr.user_id
            GROUP BY cr.course_id, cr.user_id
            ORDER BY cr.course_id, cr.user_id
            LIMIT {course_record_page_size} OFFSET {offset};
        """
        cursor.execute(sql)
        return [BasicCourseRecord(row[0], row[1], row[2], row[3]) for row in cursor.fetchall()]


def get_incomplete_course_records_with_records(records_to_query: List[CourseRecordBase]):
    logger.info(f"Fetching incomplete course records for {len(records_to_query)} learner records")
    total_records = []
    for _i in range(0, len(records_to_query), 1000):
        batch = records_to_query[_i:_i + 1000]
        logger.info(f"Finding course records {len(batch)} learner records")
        course_ids = set()
        user_ids = set()
        for record in batch:
            course_ids.add(record.course_id)
            user_ids.add(record.user_id)
        total_records.extend(get_incomplete_course_records_with_ids(course_ids, user_ids))
    return total_records


def get_incomplete_course_records_with_ids(user_ids: Set[str], course_ids: Set[str]):
    logger.info("Fetching incomplete course records")
    course_ids_in = ",".join([f"'{_id}'" for _id in course_ids])
    user_ids_in = ",".join([f"'{_id}'" for _id in user_ids])
    conn = get_mysql_connection()
    with conn.cursor() as cursor:
        sql = f"""
            SELECT cr.course_id, cr.user_id, cr.state, cr.preference, cr.last_updated
            from course_record cr
            where cr.course_id in ({course_ids_in}) and cr.user_id in ({user_ids_in}) and cr.state != 'COMPLETED';
        """
        logger.info(sql)
        cursor.execute(sql)
        return [CourseRecord(row[0], row[1], row[2], row[3], None, row[4]) for row in cursor.fetchall()]
