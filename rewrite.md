1. Remove subprocess and interactions with database, no need for us to connect to database to take dumps
users should provide the dump to use, they should dump the database themselves
2. We will only take the dump, split into into the various objects
--> We will use pglast visitors to parse the file and extract the various objects into their respective files
--> We are mostly interested in DDLs
3. Users can decide on what objects they want to process from the dumps, by default we process all/every objects
