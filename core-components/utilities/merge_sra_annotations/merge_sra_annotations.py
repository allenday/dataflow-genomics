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

src_dir = "src/"
paths = [src_dir + path for path in listdir(src_dir)]

new_path = dest_dir + "human-1k.csv"

with open(new_path, "w") as wf:
    writer = csv.writer(wf, delimiter='\t')
    writer.writerow(dest_headers)
    for path in paths:
        with open(path, "r") as f:
            reader = csv.reader(f, delimiter='\t')
            print("Working with {}".format(path))
            line_count = 0
            for line in reader:
                if line_count == 0:
                    current_headers = line
                    current_headers_lower = [current_header.lower().replace(" ", "_") for current_header in line]
                    print(current_headers)

                    new_headers = [current_headers[index] for index, current_header_lower in
                                   enumerate(current_headers_lower) if
                                   current_header_lower not in dest_headers_lower]

                    dest_headers = dest_headers + new_headers
                    dest_headers_lower = [dest_header.lower().replace(" ", "_") for dest_header in dest_headers]

                    # writer.writerow(dest_headers)

                    convert_schema = dict(
                        [(header, current_headers_lower.index(header)) for header in dest_headers_lower if
                         header in current_headers_lower])
                else:
                    new_line_elements = [
                        line[convert_schema[header]] if header in convert_schema else ''
                        for header in dest_headers_lower]
                    new_line_elements = [el.replace('NULL', '') for el in new_line_elements]
                    writer.writerow(new_line_elements)
                line_count += 1
