import argparse
import logging
import json
import csv

from os import path


def to_slugs(src_list):
    return [current_header.lower().replace(" ", "_") for current_header in src_list]


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument("dest_csv_file", help="The file to write converted CSV lines")
    parser.add_argument("--sra_run_table_file",
                        help="Source file that was generated with SRA Run Selector")
    parser.add_argument("--csv_schema_json_file", default="sra_format_schema.json",
                        help="JSON file with json schema that describes CSV schema for dest "
                             "CSV file")
    parser.add_argument("--delimiter", default="TAB")

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

    args = parser.parse_args()

    delimiter = "\t" if args.delimiter == "TAB" else ","

    if not args.csv_schema_json_file:
        logging.info('Please provide --csv_schema_json_file argument')
        exit(1)

    with open(args.csv_schema_json_file) as schema_file:
        csv_schema = json.load(schema_file)
        dest_headers = [field["name"] for field in csv_schema["fields"]]

    if not path.exists(args.dest_csv_file) or not open(args.dest_csv_file, "r").readline():
        with open(args.dest_csv_file, "w") as dest_file:
            writer = csv.writer(dest_file, delimiter=delimiter)
            writer.writerow(dest_headers)

    with open(args.dest_csv_file, "a") as dest_file:
        writer = csv.writer(dest_file, delimiter=delimiter)

        if args.sra_run_table_file:
            with open(args.sra_run_table_file, "r") as sra_run_table_file:
                sra_run_table_file_reader = csv.reader(sra_run_table_file, delimiter=',')

                line_count = 0
                for sra_run_table_line in sra_run_table_file_reader:
                    if line_count == 0:
                        current_headers_slugs = to_slugs(sra_run_table_line)
                        dest_headers_slugs = to_slugs(dest_headers)

                        convert_schema = dict(
                            [(dest_headers[dest_headers_slugs.index(header)], current_headers_slugs.index(header)) for
                             header in dest_headers_slugs if
                             header in current_headers_slugs])
                    else:
                        new_line_elements = [
                            sra_run_table_line[convert_schema[header]] if header in convert_schema else ''
                            for header in dest_headers]
                        new_line_elements = [el.replace('NULL', '') for el in new_line_elements]
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

            new_line_elements = [new_line_metadata[header] if header in new_line_metadata else ''
                                 for header in dest_headers]
            writer.writerow(new_line_elements)
        else:
            logging.info('You should provide --sra_run_table_file or add metadata manually with --manually argument')
            exit(1)
