from os import listdir
import os
import csv

dest_headers = ['AvgSpotLen', 'BioSample', 'DATASTORE_provider', 'DATASTORE_region', 'Experiment', 'InsertSize',
                'LibraryLayout', 'Library_Name', 'MBases', 'MBytes', 'Run', 'SRA_Sample', 'Sample_Name', 'Assay_Type',
                'BioProject', 'Center_Name', 'Consent', 'DATASTORE_filetype', 'Instrument', 'LibrarySelection',
                'LibrarySource', 'LoadDate', 'Organism', 'Platform', 'ReleaseDate', 'SRA_Study']
dest_headers_lower = [dest_header.lower().replace(" ", "_") for dest_header in dest_headers]

dest_dir = "converted/"
if not os.path.exists(dest_dir):
    os.mkdir(dest_dir)

paths = [path for path in listdir(".") if path.endswith("e.csv")]

for path in paths:
    new_path = dest_dir + path.replace(".csv", "_converted.csv")

    with open(path, "r") as f:
        reader = csv.reader(f, delimiter=',')
        print("Working with {}".format(path))
        with open(new_path, "w") as wf:
            writer = csv.writer(wf, delimiter='\t')

            line_count = 0
            for line in reader:
                if line_count == 0:
                    current_headers = line
                    current_headers_lower = [current_header.lower().replace(" ", "_") for current_header in line]
                    print(current_headers)

                    new_headers = [current_headers[index] for index, current_header_lower in enumerate(current_headers_lower) if
                                   current_header_lower not in dest_headers_lower]

                    dest_headers = dest_headers + new_headers
                    dest_headers_lower = [dest_header.lower().replace(" ", "_") for dest_header in dest_headers]

                    writer.writerow(dest_headers)

                    convert_schema = dict(
                        [(header, current_headers_lower.index(header)) for header in dest_headers_lower if
                         header in current_headers_lower])
                    convert_schema["sra_sample"] = convert_schema["sra_accession"]
                else:
                    new_line_elements = [
                        line[convert_schema[header]] if header in convert_schema else 'NULL'
                        for header in dest_headers_lower]
                    writer.writerow(new_line_elements)
                line_count += 1
