import datetime

from hedgepy.server.bases.Agent import Consumer, Schedule, ScheduleItem
from hedgepy.server.bases.Database import Database


async def fill_data(
    start: datetime.date, 
    end: datetime.date, 
    schedule_item: ScheduleItem, 
    database: Database, 
    consumer: Consumer
    ) -> None:
    request = schedule_item.request
    request.start = start
    request.end = end
    uuid = await consumer.post(request)
    response = await consumer.get(uuid)
    await database.query(
        which="insert_row", 
        schema=request.vendor, 
        table=request.endpoint, 
        rows=response.data
        )


async def check_existing_data(
    schedule_item: ScheduleItem, 
    database: Database
    ) -> tuple[tuple[datetime.date, datetime.date] | None, tuple[datetime.date, datetime.date] | None]:
    backfill = None
    frontfill = None
    existing = database.query(
        which='select_columns', 
        schema=schedule_item.request.vendor, 
        table=schedule_item.request.endpoint, 
        columns=('date',)
        )
    if len(existing) == 0:
        backfill = schedule_item.request.start, schedule_item.request.end
    else: 
        if min(existing) > schedule_item.request.start:
            backfill = schedule_item.request.start, min(existing)
        if max(existing) < schedule_item.request.end:
            frontfill = max(existing), schedule_item.request.end
    return backfill, frontfill


async def main(schedule: Schedule, database: Database, consumer: Consumer):
    for schedule_item in schedule.items:
        backfill, frontfill = check_existing_data(schedule_item, database)
        if backfill:
            start, end = backfill
            await fill_data(start, end, schedule_item, database, consumer)
        if frontfill:
            start, end = frontfill
            await fill_data(start, end, schedule_item, database, consumer)
    