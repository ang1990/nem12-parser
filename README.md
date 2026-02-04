# nem12-parser
A take-home assignment to read NEM12-formatted files, and generate SQL statements representing the corresponding energy consumption records.

# Installation

Install Python 3.12 or later: `brew install python`

Install the required Python packages: `pip install -r requirements.txt` 

# Operation

The parser receives CSV files containing NEM12-compliant lines,
and outputs INSERT statements according to the meter_readings table schema to STDOUT.

To run the parser on a file:

```bash
python main_parser.py nem_12_file.csv
```

# Technical Assessment Answers

### Q1. What is the rationale for the technologies you have decided to use?

I chose the following technologies:
1. Python 3.12 - Python is easy to develop in and offers extensive library support
which trivialises several well-established parts of the development process (CSV reading, SQL generation).

    I use the `csv` and `sqlalchemy` packages for CSV reading and SQL generation respectively.


### Q2. What would you have done differently if you had more time?

1. Better understand how NEM12 records are generated and used. Leading questions would be:
   >- How do you deal with meter readings that are estimated and then corrected?
   >- Can we assume weak ordering of the NEM12 records by NMI?
2. Implement switches to handle deconflicting of the insert statements.
   > The engineering issue arises from how to deal with the uniqueness constraint whilst keeping memory usage down.
   >
   > This solution opted for a compromise between the two. For big files it runs the chance of generating conflicting inserts.
   > 
   > As it is unclear to me how conflicting inserts should be handled outside of this script,
   > I opted to introduce the ON DUPLICATE KEY UPDATE handling to the insert statement. This limited me to the MySQL dialect in my implementation.
   > 
   > If I had more time I could implement switches to remove this handling, or introduce handling to generate the same deconfliction using different SQL dialects (PostgreSQL, MariaDB)
   > 
   > An alternative considered was to implement a get_and_update behavior, but ultimately left out because the parser assumes no access to the DB. Additionally, this solution could run into data loss issues in high-concurrency environments.
   

### Q3. What is the rationale for the design choices that you have made?

The assessment specified that the files could be very large. In light of this
I made some design decisions:

> The CSV reader chosen iterates through each line without storing the information from the previous line.
This lets us read the file without loading the entire file into memory.
>
> Meter readings with zero value are ignored to cut down on inserts. The assumption is that downstream can assume a value to be zero if no record is found. For billing purposes this should be sufficient.

The table schema also indicates a uniqueness constraint of NMI + timestamp. This made for a couple of design considerations:

> The parser maintains a mapping linking an NMI + timestamp to the respective consumption. 
The parser populates this mapping as it parses the document.
> 
>   The idea is to deduplicate for statements that have the same NMI and timestamp by summing up the values.

However, this runs the risk of this mapping becoming very large and overrunning local memory.

> The parser thus tracks the size of this mapping. On reaching this limit, the parser immediately outputs the insert statements and clears the mapping.
> 
> To remove issues around possible conflicting inserts from within the file, the inserts are provided an ON DUPLICATE KEY UPDATE deconfliction behavior to sum all consumption values with the unique key.
