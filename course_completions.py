from config import get_pg_connection
from log import get_logger
from models import CourseRecordBase

logger = get_logger('course_completions')


class CourseCompletion(CourseRecordBase):
    def __init__(self, course_id, user_id, event_timestamp):
        super().__init__(course_id, user_id)
        self.event_timestamp = event_timestamp


def get_course_completions():
    logger.info("Fetching course completions")
    conn = get_pg_connection()
    with conn.cursor() as cursor:
        sql = f"""
            select cce.course_id, cce.user_id, cce.event_timestamp
            from course_completion_events cce
            where cce.user_id is not NULL
            -- handle duplicates
            group by cce.course_id, cce.user_id, cce.event_timestamp
        """
        cursor.execute(sql)
        return [CourseCompletion(row[0], row[1], row[2]) for row in cursor.fetchall()]
