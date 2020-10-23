import argparse
import logging
import json
import csv

from os import path


def to_slugs(src_list):
    return [current_header.lower().replace(" ", "_") for current_header in src_list]


def generate_convert_schema(input_headers, dest_headers):
    current_headers_slugs = to_slugs(input_headers)
    dest_headers_slugs = to_slugs(dest_headers)

    return dict(
        [(dest_headers[dest_headers_slugs.index(header)], current_headers_slugs.index(header)) for
         header in dest_headers_slugs if
         header in current_headers_slugs])


def csv_schema_from_file(filepath):
    with open(filepath) as schema_file:
        csv_schema = json.load(schema_file)
        return [field["name"] for field in csv_schema["fields"]]

DATA_SOURCE_TYPE_FIELD_NAME = "DataSourceType"

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument("dest_csv_file", help="The file to write converted CSV lines")
    parser.add_argument("--dest_csv_schema_json_file", default="../../csv_schema/default_format_schema.json",
                        help="JSON file with json schema that describes CSV schema for dest "
                             "CSV file")
    parser.add_argument("--src_csv", help="Source CSV file with custom csv schema")
    parser.add_argument("--src_csv_schema", help="Take input CSV schema from JSON schema")
    parser.add_argument("--parse_schema_from_src_headers", action="store_true",
                        help="Parse result file headers from source file")
    parser.add_argument("--skip_headers_lines", default=0, type=int, help="Numbers of headers lines in src file")
    parser.add_argument("--override_data_source_type", choices=['gcs_uri_provider', 'sra'])
    parser.add_argument("--src_delimiter", default="TAB", choices=['COMMA', 'TAB'])
    parser.add_argument("--dest_delimiter", default="TAB", choices=['COMMA', 'TAB'])

    parser.add_argument("--manually", action="store_true", help="Write CSV line from manually set values")
    parser.add_argument("--run_id")
    parser.add_argument("--run_id_field_name", default="Run")
    parser.add_argument("--sample_id")
    parser.add_argument("--sample_id_field_name", default="SRA_Sample")
    parser.add_argument("--study_id")
    parser.add_argument("--study_id_field_name", default="SRA_Study")
    parser.add_argument("--paired", action="store_true")
    parser.add_argument("--is_paired_field_name", default="LibraryLayout")
    parser.add_argument("--platform")
    parser.add_argument("--platform_field_name", default="Platform")
    parser.add_argument("--data_source_type", choices=['gcs_uri_provider', 'sra', 'url', 'gcs'])
    parser.add_argument("--data_source_type_field_name", default="DataSourceType")
    parser.add_argument("--data_source_links")
    parser.add_argument("--data_source_links_field_name", default="DataSourceLinks")

    args = parser.parse_args()

    dest_delimiter = "\t" if args.dest_delimiter == "TAB" else ","
    src_delimiter = "\t" if args.src_delimiter == "TAB" else ","

    if not args.dest_csv_schema_json_file:
        logging.info('Please provide --dest_csv_schema_json_file argument')
        exit(1)

    dest_headers = csv_schema_from_file(args.dest_csv_schema_json_file)
    data_source_type_index = dest_headers.index(DATA_SOURCE_TYPE_FIELD_NAME)

    if not path.exists(args.dest_csv_file) or not open(args.dest_csv_file, "r").readline():
        with open(args.dest_csv_file, "w") as dest_file:
            writer = csv.writer(dest_file, delimiter=dest_delimiter)
            writer.writerow(dest_headers)

    with open(args.dest_csv_file, "a") as dest_file:
        writer = csv.writer(dest_file, delimiter=dest_delimiter)

        if args.src_csv:
            with open(args.src_csv, "r") as src_csv_file:
                src_csv_reader = csv.reader(src_csv_file, delimiter=src_delimiter)
                if args.src_csv:
                    line_count = 0

                    skip_headers_lines = 1 if args.parse_schema_from_src_headers else args.skip_headers_lines
                    for read_line in src_csv_reader:
                        if line_count == 0:
                            if args.src_csv_schema:
                                src_headers = csv_schema_from_file(args.src_csv_schema)
                            elif args.parse_schema_from_src_headers:
                                src_headers = read_line
                            else:
                                logging.info('Please provide --src_csv_schema argument or --parse_schema_from_src_headers args')
                                exit(1)
                            convert_schema = generate_convert_schema(src_headers, dest_headers)
                        if line_count >= skip_headers_lines:
                            new_line_elements = [read_line[convert_schema[header]] if header in convert_schema and len(read_line)>convert_schema[header] else '' for header in dest_headers]
                            new_line_elements = [el.replace('NULL', '') for el in new_line_elements]

                            if args.override_data_source_type:
                                new_line_elements[data_source_type_index] = args.override_data_source_type
                            elif data_source_type_index >=0 and not new_line_elements[data_source_type_index]:
                                logging.info('Please provide --override_data_source_type argument')
                                exit(1)
                            writer.writerow(new_line_elements)

                        line_count += 1
        elif args.manually:
            new_line_metadata = {}
            if args.run_id:
                new_line_metadata[args.run_id_field_name] = args.run_id
            else:
                logging.info('Please provide --run_id argument')
                exit(1)
            if args.sample_id:
                new_line_metadata[args.sample_id_field_name] = args.sample_id
            else:
                logging.info('Please provide --sample_id argument')
                exit(1)
            if args.study_id:
                new_line_metadata[args.study_id_field_name] = args.study_id
            else:
                logging.info('Please provide --study_id argument')
                exit(1)
            if args.platform:
                new_line_metadata[args.platform_field_name] = str(args.platform).upper()
            else:
                logging.info('Please provide --platform argument')
                exit(1)
            new_line_metadata[args.is_paired_field_name] = "PAIRED" if args.paired else "SINGLE"

            if args.data_source_type or args.override_data_source_type:
                new_line_metadata[args.data_source_links_field_name] = args.data_source_type \
                    if args.data_source_type else args.override_data_source_type
            else:
                logging.info('Please provide --override_data_source_type or --data_source_type argument')
                exit(1)
            if args.data_source_links:
                new_line_metadata[args.data_source_links_field_name] = args.data_source_links
            elif args.data_source_type == "url" or args.data_source_type == "gcs":
                logging.info('Please provide --data_source_links argument')
                exit(1)

            new_line_elements = [new_line_metadata[header] if header in new_line_metadata else ''
                                 for header in dest_headers]
            writer.writerow(new_line_elements)
        else:
            logging.info('You should provide --src_csv or add metadata manually with --manually argument')
            exit(1)
