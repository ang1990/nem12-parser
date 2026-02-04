import csv
import os
import uuid
from collections import defaultdict
from datetime import datetime, timedelta
from itertools import batched
from zoneinfo import ZoneInfo
from decimal import Decimal
from sqlalchemy import Column, String, func
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.types import DateTime, Uuid, Numeric
from sqlalchemy.dialects.mysql import insert
from typing import Iterable


# meter_readings table schema for reference
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


MINS_PER_DAY = 24 * 60
AUS_TZ = ZoneInfo('Australia/Brisbane')  # seems this tz is recommended for AEST

# change this to your DB URL, or point to the envvar for it
DATABASE_URL = os.environ.get('DATABASE_URL', 'sqlite:///:memory:')
INSERT_BATCH_SIZE = 100
CONSUMPTION_MAPPING_SIZE_LIMIT = 10 ** 5


def _is_decimal_value(s: str) -> bool:
    parts = s.split('.')
    return all(not part or part.isnumeric() for part in parts)


class Base(DeclarativeBase):
    pass


class MeterReading(Base):
    __tablename__ = 'meter_readings'

    id = Column(Uuid, primary_key=True, default=uuid.uuid4, nullable=False)
    nmi = Column(String(10), nullable=False)
    timestamp = Column(DateTime(timezone=True), nullable=False)
    consumption = Column(Numeric(precision=6, scale=3), nullable=False)


def parse(filepath: str):
    """
        fp: File Pointer
        Algorithm:
        1. Read the header
        2. Read the 200s and 300s
            - Generate the NMIs and consumptions
            - Aggregate the consumption values per NMI
        3. Generate SQL from the NMIs and consumptions
    """
    with open(filepath, 'r') as file:
        reader = csv.reader(file, delimiter=',')
        header_row = next(reader)
        assert header_row[0] == '100', 'Header malformed or missing'
        assert header_row[1] == 'NEM12', 'Only NEM12 format is supported'

        for meter_reading_batch in batched(get_meter_readings(reader), n=INSERT_BATCH_SIZE):
            insert_stmt = insert(MeterReading).values([{
                'nmi': mr.nmi,
                'timestamp': mr.timestamp,
                'consumption': mr.consumption
            } for mr in meter_reading_batch])

            on_duplicate_key_stmt = insert_stmt.on_duplicate_key_update(
                consumption=(MeterReading.consumption + insert_stmt.inserted.consumption)
            )
            compiled_statement = on_duplicate_key_stmt.compile(compile_kwargs={"literal_binds": True})
            yield str(compiled_statement)


def get_meter_readings(file_reader) -> Iterable[MeterReading]:
    yield from parse_records(file_reader)


def parse_records(file_reader) -> Iterable[MeterReading]:
    consumption_records_agg = defaultdict(lambda: defaultdict(Decimal))
    curr_nmi = ''
    time_interval: int = 0
    records_count = 0

    for row in file_reader:
        match row[0]:
            case '200':
                # 200 statement, we switch NMI context
                curr_nmi = row[1]
                time_interval = int(row[-2])
            case '300':
                assert curr_nmi, '300 record encountered before 200 record'
                interval_date = datetime.strptime(row[1], '%Y%m%d').astimezone(AUS_TZ)
                interval_count = row.index(next(elem for elem in row[2:] if not _is_decimal_value(elem))) - 2
                for idx in range(interval_count):
                    curr_timestamp = interval_date + timedelta(minutes=time_interval * idx)
                    if reading := Decimal(row[idx+2]):
                        if consumption_records_agg[curr_nmi][curr_timestamp] > 0:
                            consumption_records_agg[curr_nmi][curr_timestamp] += Decimal(reading)
                        else:
                            records_count += 1
                            consumption_records_agg[curr_nmi][curr_timestamp] = Decimal(reading)

                        if records_count >= CONSUMPTION_MAPPING_SIZE_LIMIT:
                            for nmi, timestamped_records in consumption_records_agg.items():
                                for timestamp, consumption in timestamped_records.items():
                                    yield MeterReading(nmi=nmi, timestamp=timestamp, consumption=consumption)
                            consumption_records_agg.clear()
                            records_count = 0
            case '400':
                # we don't need 400s yet
                pass
            case '500':
                curr_nmi = ''
                time_interval = 0
                pass
            case '900':
                break

    # clear the remaining for low mem mode (or the whole thing for the agg mode)
    for nmi, timestamped_records in consumption_records_agg.items():
        for timestamp, consumption in timestamped_records.items():
            yield MeterReading(nmi=nmi, timestamp=timestamp, consumption=consumption)


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('filepath', help='The name of the file to parse.')
    parser.add_argument('--output_file', nargs='?', type=str, default='',
                        help='Name of the file to write the insert statements to. '
                             'Ignore if you do not want the parser to write to a file.'
                             'Alternative you could pipe STDOUT to a file instead.')
    args = parser.parse_args()
    if args.output_file:
        with open(args.output_file, 'w') as output_file:
            for insert_stmt in parse(args.filepath):
                output_file.write(insert_stmt)
    else:
        for r in parse(args.filepath):
            print(r)
