# migrate-course-records

## Overview

Migrate `course_record` data into the `learner_records` table.

### Learner record migration

The script will find records in the `learner_records.course_record` table and insert them as new learner records into
the `learner_record.learner_records` table.

It will attempt to find the created timestamp by either selecting the earliest module record `created_at` date for that
record **or** if there are no module records, the `last_updated` date for the course record itself.

### Learner record event migration

Course completions from the `reporting.course_completion_events` table will be queried and inserted as `COMPLETE_COURSE`
events for each learner record. Multiple completions can be inserted.

If a learner record **does not** have any completion events associated to it (and the course record is NOT
`state=COMPLETED`), the script will attempt to create a
`MOVE_TO_LEARNING_PLAN`, `REMOVE_FROM_LEARNING_PLAN` or `REMOVE_FROM_SUGGESTIONS` event, based on the `state` and
`preference` of the course record.

## Setup

As always, first run `pip install -r requirements.txt`

Set the following properties in a `.env` file (or as system env vars):

- `EVENT_SOURCE_ID` (CSL event source ID)
- `MYSQL_HOST`
- `MYSQL_USER`
- `MYSQL_PASSWORD`
- `PG_HOST`
- `PG_PASSWORD`
- `PG_USER`

Optionally set the following properties:

- `COURSE_RECORD_PAGE_SIZE` (page size to use when querying course records)

## Run

The script uses the following arguments:

| Argument         | Description                                                                                  | Choices                         | Default           | Example Usage            |
|:-----------------|:---------------------------------------------------------------------------------------------|:--------------------------------|:------------------|:-------------------------|
| **`data_types`** | Specifies one or more data types (tables) to process. Separate multiple choices with spaces. | `learner_records`, `events`     | *None* (Required) | `learner_records events` |
| **`action`**     | Defines the operation to perform with the specified data.                                    | `report`, `execute`, `teardown` | `report`          | `--action execute`       |                           

### Example usage

To report on learner_record migration:
`python script.py learner_records --action report`

To execute learner_record_event migration:
`python script.py events --action execute`

To teardown the learner_record_event table:
`python script.py events --action teardown`