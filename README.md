# nem12-parser
A take-home assignment to read NEM12-formatted files, and generate SQL statements representing the corresponding energy consumption records.

# Installation

Make sure you have the following installed first:
- Python 3.12 or later

Run `pip install -r requirements.txt` 

# Operation

The parser receives CSV files containing NEM12-compliant lines,
and outputs INSERT statements according to the meter_readings table schema.

To run the parser on a file:

```bash
python main_parser.py nem_12_file.csv
```

# Technical Assessment Answers

### Q1. What is the rationale for the technologies you have decided to use?

I chose the following technologies:
1. Python 3.12 - Python is easy to develop in and offers extensive library support
which trivialises many parts of the development process (CSV reading, SQL generation).

    I use the `csv` and `sqlalchemy` packages for CSV reading and SQL generation respectively.


### Q2. What would you have done differently if you had more time?

1. Study the impact of the INSERT statements on the database.

### Q3. What is the rationale for the design choices that you have made?

The assessment specified that the files could be very large. In light of this
I made some design decisions:

- Files larger than 2GB are 