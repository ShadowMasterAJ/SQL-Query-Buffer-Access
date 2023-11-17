import os
import csv

def convert_tbl_to_csv(input_path, output_path):
    with open(input_path, 'r') as tbl_file, open(output_path, 'w', newline='') as csv_file:
        tbl_reader = csv.reader(tbl_file, delimiter='|')
        csv_writer = csv.writer(csv_file)

        for row in tbl_reader:
            cleaned_row = [item for item in row if item]
            csv_writer.writerow(cleaned_row)

def process_tbl_files_in_directory(input_directory, output_directory):
    for filename in os.listdir(input_directory):
        if filename.endswith(".tbl"):
            input_path = os.path.join(input_directory, filename)
            output_path = os.path.join(output_directory, filename.replace(".tbl", ".csv"))
            convert_tbl_to_csv(input_path, output_path)

if __name__ == "__main__":
    input_data_directory = "data/tables"
    output_data_directory = "data/cleaned"

    if not os.path.exists(output_data_directory):
        os.makedirs(output_data_directory)

    process_tbl_files_in_directory(input_data_directory, output_data_directory)
