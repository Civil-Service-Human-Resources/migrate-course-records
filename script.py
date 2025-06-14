import argparse
from typing import List, Dict

from course_completions import get_course_completions, CourseCompletion
from learner_record import get_course_records, CourseRecord, LearnerRecord, \
    LearnerRecordWithEvents, get_all_learner_records, LearnerRecordEvent, COMPLETE_COURSE, \
    get_incomplete_course_records_with_records, REMOVE_FROM_LEARNING_PLAN, MOVE_TO_LEARNING_PLAN, \
    REMOVE_FROM_SUGGESTIONS, insert_learner_record_events, insert_learner_records, delete_learner_records, \
    delete_learner_record_events, get_user_course_record_counts, get_user_learner_record_counts
from log import get_logger

logger = get_logger('script')


def transform_course_records_into_learner_records(course_records: List[CourseRecord]):
    return [LearnerRecord(record.course_id, record.user_id, 0, record.created_at) for record in
            course_records]


def get_missing_user_ids_to_fetch():
    logger.info("Counting user course records")
    course_record_counts = get_user_course_record_counts()
    learner_record_counts = get_user_learner_record_counts()
    missing_learner_ids = []
    logger.info("Finding missing records via learner_id")
    for learner_id, course_record_count in course_record_counts.items():
        learner_record_count = learner_record_counts.get(learner_id)
        if not learner_record_count or learner_record_count > course_record_count:
            missing_learner_ids.append(learner_id)

    logger.info(f"{len(missing_learner_ids)} missing learner ids")
    return missing_learner_ids


def insert_course_records_for_missing_users(missing_learner_ids: List[str], execute=False):
    for _i in range(0, len(missing_learner_ids), 2000):
        batch = missing_learner_ids[_i:_i + 2000]
        result = get_course_records(batch)
        learner_records = transform_course_records_into_learner_records(result)
        if execute:
            logger.info(f"Inserting {len(learner_records)} learner records")
            insert_learner_records(learner_records)
        else:
            logger.info("execute flag not passed. Not inserting")


def fetch_all_lr_map():
    learner_records = get_all_learner_records()
    logger.info(f"Fetched {len(learner_records)} learner records")
    return {lr.get_id(): lr for lr in learner_records}


def transform_course_record_into_event_id(lr: LearnerRecord, course_record: CourseRecord):
    if course_record.state == 'ARCHIVED':
        if lr.created_timestamp != course_record.created_at:
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
    logger.info(f"Processing {len(course_completions)} course completion events")
    completions_processed = 0
    for completion in course_completions:
        course_record_id = completion.get_id()
        lr = learner_records.get(course_record_id)
        if lr:
            lre = LearnerRecordEvent(lr.lr_id, COMPLETE_COURSE, completion.event_timestamp)
            lr.events.append(lre)
            lr.has_completions = True
            learner_records[course_record_id] = lr
            completions_processed += 1
        else:
            logger.warning(f"Learner record with id {course_record_id} doesn't exist")
    logger.info(f"Processed {completions_processed} out of {len(course_completions)} course completion events")
    return learner_records


def apply_non_completion_events(learner_records: Dict[str, LearnerRecordWithEvents]):
    non_completion_records = [lr for lr in learner_records.values() if not lr.has_completions]
    incomplete_records = get_incomplete_course_records_with_records(non_completion_records)
    return find_non_completion_events(learner_records, incomplete_records)


def find_non_completion_events(learner_records: Dict[str, LearnerRecordWithEvents],
                               incomplete_records: List[CourseRecord]):
    logger.info(f"Processing other events for {len(incomplete_records)} incomplete course records")
    events_processed = 0
    for incomplete_record in incomplete_records:
        course_record_id = incomplete_record.get_id()
        lr = learner_records.get(course_record_id)
        if lr:
            event_id = transform_course_record_into_event_id(lr, incomplete_record)
            if event_id:
                lre = LearnerRecordEvent(lr.lr_id, event_id, incomplete_record.last_updated)
                lr.events.append(lre)
                learner_records[course_record_id] = lr
                events_processed += 1
        else:
            logger.warning(f"Learner record with id {course_record_id} doesn't exist")
    logger.info(f"Processed {events_processed} out of {len(incomplete_records)} incomplete course records")
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
        missing_learner_ids = get_missing_user_ids_to_fetch()
        insert_course_records_for_missing_users(missing_learner_ids, execute)

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
