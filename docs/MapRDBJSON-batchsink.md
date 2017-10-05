# MapR-DB JSON Table sink

Description
-----------
MapR-DB JSON table sink is used to write the JSON documents to the MapR-DB table.

Properties
----------

**tableName:** Path to the MapR-DB JSON table. Table must exist in the MapR-DB.

**key:** Field in the record to be used as a id for the JSON document while storing the document in the table.
Only 'String' types are supported as id.

**schema:** Output schema for the JSON document.