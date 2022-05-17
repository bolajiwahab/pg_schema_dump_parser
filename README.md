# pg_schema_dump_parser
Generates nicely parsed schema files.

## Requirements
- Requires at least python3

## Sample parsed schema
![plot](sample_schema.png)

## Running the program
- Create `pg_schema_dump.config` with template `pg_schema_dump.config.sample` replacing the necessary values
- Then you can call the program as such:
  ```
  ./pg_schema_dump_parser.py --directory . --configfile pg_schema_dump.config
  ```
  P.S we are dumping the schema files into the current directory(.)
