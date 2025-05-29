from typing import List


class CourseRecordPagination:
    def __init__(self, course_record_max_page_size: int, course_record_count: int, total_course_record_pages: int,
                 processed_count: int, total_processed_pages: int):
        self.course_record_max_page_size = course_record_max_page_size
        self.course_record_count = course_record_count
        self.total_course_record_pages = total_course_record_pages
        self.processed_count = processed_count
        self.total_processed_pages = total_processed_pages

    def get_remaining_records(self):
        return self.course_record_count - self.processed_count

    def has_remaining_records(self):
        return self.get_remaining_records() > 0

    def get_remaining_pages(self):
        return self.total_course_record_pages - self.total_processed_pages


class CourseRecordBase:
    def __init__(self, course_id: str, user_id: str):
        self.course_id = course_id
        self.user_id = user_id

    def get_id(self):
        return f"{self.course_id},{self.user_id}"


def course_records_to_map(records: List[CourseRecordBase]):
    return {r.get_id(): r for r in records}
