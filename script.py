import argparse
import math
from typing import List, Dict

from config import course_record_page_size
from course_completions import get_course_completions, CourseCompletion
from learner_record import get_course_records, count_course_records, count_learner_records, CourseRecord, LearnerRecord, \
    LearnerRecordWithEvents, get_all_learner_records, LearnerRecordEvent, COMPLETE_COURSE, \
    get_incomplete_course_records_with_records, REMOVE_FROM_LEARNING_PLAN, MOVE_TO_LEARNING_PLAN, \
    REMOVE_FROM_SUGGESTIONS, insert_learner_record_events, insert_learner_records, delete_learner_records, \
    delete_learner_record_events
from log import get_logger
from models import CourseRecordPagination

logger = get_logger('script')


def transform_course_records_into_learner_records(course_records: List[CourseRecord]):
    return [LearnerRecord(record.course_id, record.user_id, 0, record.created_at) for record in
            course_records]


def get_course_record_pagination():
    logger.info("Counting course records")
    course_record_count = count_course_records()
    max_page_count = math.ceil(course_record_count / course_record_page_size)
    logger.info(
        f"There are {course_record_count} course records and {max_page_count} pages (with a page size of {course_record_page_size})")

    logger.info("Counting learner records")
    learner_record_count = count_learner_records()
    learner_record_pages = learner_record_count // course_record_page_size
    logger.info(
        f"There are {learner_record_count} learner records. {learner_record_pages} full pages have been processed")
    return CourseRecordPagination(course_record_page_size, course_record_count, max_page_count, learner_record_count,
                                  learner_record_pages)


def insert_course_records(pagination: CourseRecordPagination, execute=False):
    if pagination.has_remaining_records():
        logger.info(
            f"There are {pagination.get_remaining_records()} records left to process, which is {pagination.get_remaining_pages()} pages")
        for page in range(pagination.total_processed_pages, pagination.total_course_record_pages):
            page = page + 1
            logger.info(f"Fetching course records for page {page}")
            result = get_course_records(page)
            logger.info(f"Transforming {len(result)} course records")
            pagination.processed_count += len(result)
            learner_records = transform_course_records_into_learner_records(result)
            logger.info(f"{len(learner_records)} learner records ready to be inserted")
            if execute:
                logger.info(f"Inserting {len(learner_records)} learner records")
                insert_learner_records(learner_records)
            else:
                logger.info("execute flag not passed. Not inserting")
    else:
        logger.info("No records left to process")


def fetch_all_lr_map():
    learner_records = get_all_learner_records()
    logger.info(f"Fetched {len(learner_records)} learner records")
    return {lr.get_id(): lr for lr in learner_records}


def transform_course_record_into_event_id(course_record: CourseRecord):
    if course_record.state == 'ARCHIVED':
        return REMOVE_FROM_LEARNING_PLAN
    elif course_record.state is None:
        if course_record.preference == 'LIKED':
            return MOVE_TO_LEARNING_PLAN
        elif course_record.preference == 'DISLIKED':
            return REMOVE_FROM_SUGGESTIONS
    return None


def apply_course_completion_events(learner_records: Dict[str, LearnerRecordWithEvents]):
    course_completions = get_course_completions()
    return find_course_completion_events(learner_records, course_completions)


def find_course_completion_events(learner_records: Dict[str, LearnerRecordWithEvents],
                                  course_completions: List[CourseCompletion]):
    logger.info("Processing course completion events")
    for completion in course_completions:
        course_record_id = completion.get_id()
        lr = learner_records.get(course_record_id)
        if lr:
            lre = LearnerRecordEvent(lr.lr_id, COMPLETE_COURSE, completion.event_timestamp)
            lr.events.append(lre)
            lr.has_completions = True
            learner_records[course_record_id] = lr
        else:
            logger.warning(f"Learner record with id {course_record_id} doesn't exist")
    return learner_records


def apply_non_completion_events(learner_records: Dict[str, LearnerRecordWithEvents]):
    non_completion_records = [lr for lr in learner_records.values() if not lr.has_completions]
    incomplete_records = get_incomplete_course_records_with_records(non_completion_records)
    return find_non_completion_events(learner_records, incomplete_records)


def find_non_completion_events(learner_records: Dict[str, LearnerRecordWithEvents],
                               incomplete_records: List[CourseRecord]):
    logger.info("Processing other events")
    for incomplete_record in incomplete_records:
        event_id = transform_course_record_into_event_id(incomplete_record)
        if event_id:
            course_record_id = incomplete_record.get_id()
            lr = learner_records.get(course_record_id)
            if lr:
                lre = LearnerRecordEvent(lr.lr_id, event_id, incomplete_record.last_updated)
                lr.events.append(lre)
                learner_records[course_record_id] = lr
            else:
                logger.warning(f"Learner record with id {course_record_id} doesn't exist")
    return learner_records


def extract_events(_map: Dict[str, LearnerRecordWithEvents]):
    _map = apply_course_completion_events(_map)
    _map = apply_non_completion_events(_map)
    events = []
    for learner_record in _map.values():
        learner_record.sort_events()
        events.extend(learner_record.events)
    return events


def run(data: List[str], execute: bool):
    if "learner_records" in data:
        logger.info("learner_records flag found")
        course_record_pagination = get_course_record_pagination()
        insert_course_records(course_record_pagination, execute)

    if "events" in data:
        logger.info("events flag found")
        _map = fetch_all_lr_map()
        if _map.values():
            events = extract_events(_map)
            logger.info(f"{len(events)} events ready to be inserted")
            if execute:
                insert_learner_record_events(events)
            else:
                logger.info("execute flag not passed. Not inserting")
        else:
            logger.warning("0 learner records found. Not inserting any events")


def teardown(data: List[str]):
    logger.info("Tearing down data")
    if "events" in data:
        delete_learner_record_events()
    if "learner_records" in data:
        delete_learner_records()


def get_args():
    parser = argparse.ArgumentParser(description="Process")
    valid_data_choices = ["learner_records", "events"]
    parser.add_argument(
        "data_types",
        nargs='+',
        choices=valid_data_choices,
        help=f"Specify a space-separated list of data types to process. valid choices are {valid_data_choices}"
    )

    valid_action_choices = ["report", "execute", "teardown"]
    parser.add_argument(
        "action",
        choices=valid_action_choices,
        default="report",
        help=f"Specify the action to perform: valid choices are {valid_action_choices}."
    )

    return parser.parse_args()


if __name__ == "__main__":
    args = get_args()
    if args.action == "teardown":
        teardown(args.data_types)
    else:
        run(args.data_types, args.action == "execute")
