from typing import List


class CourseRecordBase:
    def __init__(self, course_id: str, user_id: str):
        self.course_id = course_id
        self.user_id = user_id

    def get_id(self):
        return f"{self.course_id},{self.user_id}"


def course_records_to_map(records: List[CourseRecordBase]):
    return {r.get_id(): r for r in records}
