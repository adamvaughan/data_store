# Data Store

An event-based time/value data store. All data is stored based on a UUID given
to the measurement being stored.

## Request Types

There are two types of requests, GET and PUT.

### GET Requests

GET requests query the data store for data. A GET request looks like the following:

    | Request Type (1 for GET) |    UUID    | Record Count (0 for GET) | Start Time (integer) | End Time (integer) |
    |        (4 bytes)         | (40 bytes) |        (4 bytes)         |      (4 bytes)       |     (4 bytes)      |

### PUT Requests

PUT requests store data in the data store. A PUT request looks like the following:

    | Request Type (2 for PUT) |    UUID    |   Record Count (integer) | Record Time (integer) | Record Value (float) |
    |        (4 bytes)         | (40 bytes) |         (4 bytes)        |       (4 bytes)       |      (4 bytes)       |

Note that even though a null-terminated UUID would only be 37 bytes, 40 bytes are
allocated for it in the request. This is for compatibility with C structs.

If a single record is being stored, its timestamp and value can be sent along with
the request. If more than one record is being stored, the last two fields of the
request are ignored and all records are expected to follow the request. A record
looks like the following:

    | Time (integer) | Value (float) |
    |   (4 bytes)    |   (4 bytes)   |

## Responses

Responses to PUT requests return the number of records that were stored:

    | Record Count (integer) |
    |       (4 bytes)        |

Responses to GET requests return the number of matching records found. The format
matches that for PUT requests. The response is then followed by the record data.
Each record is formatted the same as described above.

## Data Files

Data is stored in files based on the UUID and the time range of the data they hold.
A top-level directory is created for each year. Data files are stored two levels
deep in the year directory. The directory names are the first pair and second pair
of characters from the UUID. The file is then named as the UUID with the start
day and end day joined by underscores. For example:

    /path/to/data/2012/b7/31/b731a7d0-774b-012f-582a-482a14096e91_003_006

Each file begins with a header that contains the time fot the first and last records contained in the file:

    |  Start Time (integer) | End Time (integer) |
    |       (4 bytes)       |     (4 bytes)      |

Followed by the records. The record format on disk is the same as the record format described above.

## Configuration

Configuration of the data store server occurs when starting the server:

    DataStore::Server.start('127.0.0.1', 3490) do |config|
        config.data_directory = '/path/to/data'
        config.max_days_per_file = 5
    end

The `data_directory` setting defines the directory where all data will be stored.

The `max_days_per_file` setting determines the maximum number of days data to store
in a single file.
