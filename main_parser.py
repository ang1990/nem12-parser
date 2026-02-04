import csv
import os
import uuid
from collections import defaultdict
from datetime import datetime, timedelta
from itertools import batched
from zoneinfo import ZoneInfo
from decimal import Decimal
from sqlalchemy import Column, String, func
from sqlalchemy.orm import DeclarativeBase, Session
from sqlalchemy.types import DateTime, Uuid, Numeric
from sqlalchemy.dialects.mysql import insert
from typing import Iterable


"""
create table meter_readings ( 
id uuid default gen_random_uuid() not null, 
"nmi" varchar(10) not null, 
"timestamp" timestamp not null, 
"consumption" numeric not null, 
constraint meter_readings_pk primary key (id), 
constraint meter_readings_unique_consumption unique ("nmi", "timestamp") 
);
"""


GB = 10 ** 9
MINS_PER_DAY = 24 * 60
AUS_TZ = ZoneInfo('Australia/Brisbane')  # seems this tz is recommended for AEST

# change this to your DB URL, or point to the envvar for it
DATABASE_URL = os.environ.get('DATABASE_URL', 'sqlite:///:memory:')
DB_BATCH_SIZE = 100


class Base(DeclarativeBase):
    pass


class MeterReading(Base):
    __tablename__ = 'meter_readings'

    id = Column(Uuid, primary_key=True, default=uuid.uuid4, nullable=False)
    nmi = Column(String(10), nullable=False)
    timestamp = Column(DateTime(timezone=True), nullable=False)
    consumption = Column(Numeric(precision=6, scale=3), nullable=False)


def get_engine():
    from sqlalchemy import create_engine
    return create_engine(DATABASE_URL)


def parse(filepath: str):
    """
        fp: File Pointer
        Algorithm:
        1. Read the header
        2. Alternate between 200s and 300s
            - Generate the NMIs and consumptions
            - Aggregate the consumption values per NMI
        3. Generate SQL from the NMIs and consumptions
    """
    file_size = os.path.getsize(filepath)
    with open(filepath, "r") as file:
        reader = csv.reader(file, delimiter=",")
        header_row = next(reader)
        assert header_row[0] == 100, 'Header malformed or missing'
        assert header_row[1] == 'NEM12', 'Only NEM12 format is supported'

        for meter_reading_batch in batched(get_meter_readings(reader, file_size), n=DB_BATCH_SIZE):
            insert_stmt = insert(MeterReading).values([{
                'nmi': mr.nmi,
                'timestamp': mr.timestamp,
                'consumption': mr.consumption
            } for mr in meter_reading_batch])
            on_duplicate_key_stmt = insert_stmt.on_duplicate_key_update(
                consumption=func.sum(func.distinct(insert_stmt.inserted.consumption))
            )
            compiled_statement = on_duplicate_key_stmt.compile()
            print(compiled_statement)
            yield compiled_statement


def get_meter_readings(file_reader, file_size: int) -> Iterable[MeterReading]:
    if file_size < 2 * GB:  # 2GB to keep memory usage down
        consumption_records_agg = parse_records_with_aggregation(file_reader)
        for nmi, timestamped_records in consumption_records_agg.items():
            for timestamp, consumption in timestamped_records.items():
                yield MeterReading(nmi=nmi, timestamp=timestamp, consumption=consumption)
    else:
        yield from parse_records_low_memory(file_reader)


def parse_records_low_memory(file_reader):
    curr_nmi: str = ''
    time_interval: int = 0
    consumption_records_agg = defaultdict(lambda: defaultdict(Decimal))
    records_count = 0
    for row in file_reader:
        match row[0]:
            case 200:
                # 200 statement, we switch NMI context
                curr_nmi = row[1]
                time_interval = row[-2]
            case 300:
                assert curr_nmi, '300 record encountered before 200 record'
                # stick to this timezone so we don't have DST problems
                interval_date = datetime.strptime(row[1], '%Y%m%d').astimezone(AUS_TZ)
                interval_count = row.index(next(elem for elem in row if elem.isalpha())) - 2
                for idx in range(interval_count):
                    curr_timestamp = interval_date + timedelta(minutes=time_interval * idx)
                    if reading := row[idx+2]:
                        consumption = Decimal(reading)
                        if consumption_records_agg[curr_nmi][curr_timestamp] > 0:
                            consumption_records_agg[curr_nmi][curr_timestamp] += consumption
                        else:
                            consumption_records_agg[curr_nmi][curr_timestamp] = consumption
                            records_count += 1

                        if records_count >= DB_BATCH_SIZE:
                            for nmi, timestamped_records in consumption_records_agg.items():
                                for timestamp, consumption in timestamped_records.items():
                                    yield MeterReading(nmi=nmi, timestamp=timestamp, consumption=consumption)
                            consumption_records_agg.clear()
                            records_count = 0
            case 400:
                # we don't have a use case for 400 yet
                pass
            case 500:
                curr_nmi = ''
                time_interval = 0
                pass
            case 900:
                break


def parse_records_with_aggregation(file_reader) -> defaultdict:
    consumption_records_agg = defaultdict(lambda: defaultdict(Decimal))
    curr_nmi = ''
    time_interval: int = 0

    for row in file_reader:
        match row[0]:
            case 200:
                # 200 statement, we switch NMI context
                curr_nmi = row[1]
                time_interval = row[-2]
            case 300:
                assert curr_nmi, '300 record encountered before 200 record'
                interval_date = datetime.strptime(row[1], '%Y%m%d').astimezone(AUS_TZ)
                interval_count = row.index(next(elem for elem in row if elem.isalpha())) - 2
                for idx in range(interval_count):
                    curr_timestamp = interval_date + timedelta(minutes=time_interval * idx)
                    if reading := row[idx+2]:
                        consumption_records_agg[curr_nmi][curr_timestamp] += Decimal(reading)
            case 400:
                # we don't have a use case for 400 yet
                pass
            case 500:
                curr_nmi = ''
                time_interval = 0
                pass
            case 900:
                break

    return consumption_records_agg


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("filepath", help="The name of the file to parse.")
    args = parser.parse_args()
    parse(filepath=args.filepath)
