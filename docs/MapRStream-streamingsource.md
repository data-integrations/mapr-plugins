# MapR Streaming Source


Description
-----------
MapR streaming source. Reads events from MapR stream.


Use Case
--------
This source is used whenever you want to read from MapR Stream. For example, to read messages 
from MapR Stream and write them to HDFS or CDAP Table dataset.


Properties
----------
**referenceName:** This will be used to uniquely identify this source for lineage, annotating metadata, etc.

**topics:** The MapR Stream comma separated list of topics to read from. Example: /mapr-stream-name:mapr-stream-topic-name (Macro-enabled)

**offsetField:** The MapR Stream offset to start reading from the stream (Beginning/latest).
 Default value is latest. (Macro-enabled)

**schema:** Output schema of the source. For example if the format of the message is csv then the schema 
will contain list of fields.

**format:** Optional format of the MapR Stream event message. Any format supported by CDAP is supported.
For example, a value of 'csv' will attempt to parse MapR Stream payloads as comma-separated values.
If no format is given, MapR Stream message payloads will be treated as bytes.

Example
-------
This example reads from the 'purchases' topic of a MapR Stream 'stream'. 
It parses the MapR Stream messages using the 'csv' format
with 'user', 'item', 'count', and 'price' as the message schema.

    {
        "name": "MapRStream",
        "type": "streamingsource",
        "properties": {
            "topics": "/stream:purchases",
            "format": "csv",
            "offsetField": "latest",
            "schema": "{
                \"type\":\"record\",
                \"name\":\"purchase\",
                \"fields\":[
                    {\"name\":\"readTime\",\"type\":\"long\"},
                    {\"name\":\"key\",\"type\":\"bytes\"},
                    {\"name\":\"user\",\"type\":\"string\"},
                    {\"name\":\"item\",\"type\":\"string\"},
                    {\"name\":\"count\",\"type\":\"int\"},
                    {\"name\":\"price\",\"type\":\"double\"}
                ]
            }"
        }
    }

For each Map message read, it will output a record with the schema:

    +================================+
    | field name  | type             |
    +================================+
    | key         | bytes            |
    | user        | string           |
    | item        | string           |
    | count       | int              |
    | price       | double           |
    +================================+
