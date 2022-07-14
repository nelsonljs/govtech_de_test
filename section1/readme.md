## Section 1

### Data Exploration

Explore data that is staged. 2 columns of data is expected and data is received as a csv.

Expected data types:

| <!-- -->    | <!-- -->    |
|---|---|
| name | string |
| price | numeric |

For name field: 

- Some honorifics can be observed. Remove them as an assumption that it is not a first name/last name.
- Can names have no last name? Currently caught as error, in case database does not accept.
- Potentially capture middle names, as something to address? Do we want it?
- Capture potential other forms of honorifics.
- Names with a Jr. or Sr., DQ to confirm how we can handle these names.

For price field:
- Received number is assumed to be numeric. If failed, output poor rows for downstream processing.

### Pipeline design

Since there is no aggregation step expected. Prefer to write processing function as a map function for potential future scaling. 

Currently no intention to support as data is received daily anyway, so expected slow moving data inputs.

Since data available at 1am daily, schedule for run at 1.05am.

Erroneous rows are compiled into **error.csv** for further processing.

### ETL Pipeline

Actual ETL pipeline is packaged into a module for portability. Easier management for implementation on different automation tools.