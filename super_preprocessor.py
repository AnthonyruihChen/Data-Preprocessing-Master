#######################################################################
#### Import packages


import os
import sys
import io
import datetime
import csv
import pandas as pd

######################################################################


### Directory, Input & Output Setting:
print("Your current working directory is: ", os.getcwd(), sep="\n")

input_dir = r'~\Projects\~\batch1'
print("Please double check your input directory is: ", input_dir, sep="\n")

output_dir = r'~\Projects\~\batch1_output'
if input_dir == output_dir:
    print("The output dir cannot be same as input dir")
else:
    print("Please double check your output directory is: ", output_dir, sep="\n")


### Change cwd
os.chdir(input_dir)
print("Now, your current working directory is: ", os.getcwd(), " which should be same as input_dir", sep="\n")


### Setting some parameter
keep_header_TorF = False
file_header = ['A', 'B', 'C', 'D', 'E']

print(" Your header is: ", file_header, sep="\n")

## =====================================================================================================



### Run this part after previous part: Encoding Recommendation

enco_list = ["utf-8", "latin-1", "ISO8859-9"]
enco = input("""What is your files encoding? (eg: utf-8, latin-1, Cp1256 ...etc)""")
print(" Your guessed encoding is: ", enco, sep="\n")
print("If you are not sure, please refer to the following website:",
      "http://scratchpad.wikia.com/wiki/Character_Encoding_Recommendation_for_Languages", sep="\n")

if enco in enco_list:
    enco_i = enco_list.index(enco)
    enco_list[enco_i], enco_list[0] = enco_list[0], enco_list[enco_i]
    print("The encoding test sequence will be: ", enco_list, sep="\n")
else:
    enco_list.insert(0, enco)
    print("The encoding test sequence will be: ", enco_list, sep="\n")

## encoding recommendation:
start_time = datetime.datetime.now()
for e in enco_list:
    print(e)
    try:
        for f in os.listdir(input_dir):
            temp_df = pd.read_csv(f, encoding=e, nrows=10)
            print(f + " can be read by " + e)
        print("------------------------------------------")
        print("The recommended encoding option is: " + e)
        print("================= DONE ===================")
        break
    except:
        print(f + " !!CANNOT!! be read by " + e)
        print("Trying next encoding option")
        print("------------------Fail--------------------")

end_time = datetime.datetime.now()
print("Processing time: " + str(end_time - start_time))
## =====================================================================================================



### Run this part after confirugring previous parameters

#### Input your file format
file_format = input(
    """What is your file's format? (Please only enter one of the following: .txt or .csv or .xls or .xlsx)""")
print("Your file format is:", file_format, sep="\n")

#### Double check every output path
output_file_path = output_dir + '\Final_Cleaned_ALL.csv'
print(output_file_path)
count_file_path = output_dir + '\Count_Validation.csv'
print(count_file_path)
truncation_file_path = output_dir + '\Truncation_Validation.csv'
print(truncation_file_path)
error_file_path = output_dir + "\Error_logs.csv"
print(error_file_path)

print("Double check input path: ", input_dir, sep="\n")

if file_format == ".csv":

    print("You are running preprocessor specific for CSV files.")

    ## Track the time
    start_time = datetime.datetime.now()
    print(start_time)
    print(
        "----------------------------------------------------------------------------------------------------------------------------------------")

    with open(output_file_path, 'a', newline='') as outfile:
        csvOut = csv.writer(outfile, delimiter=',', quoting=csv.QUOTE_ALL, lineterminator='\n')
        csvOut.writerow(file_header)

        with open(count_file_path, 'w', newline='') as outfile2:
            csvOut2 = csv.writer(outfile2, delimiter=',', quoting=csv.QUOTE_ALL, lineterminator='\n')
            csvOut2.writerow(["File_Name", "Count", "Empty Rows"])

            with open(truncation_file_path, 'w', newline='') as outfile3:
                csvOut3 = csv.writer(outfile3, delimiter=',', quoting=csv.QUOTE_ALL, lineterminator='\n')
                csvOut3.writerow(file_header)

                with open(error_file_path, 'w', newline='') as outfile4:
                    csvOut4 = csv.writer(outfile4, delimiter=',', quoting=csv.QUOTE_ALL, lineterminator='\n')
                    csvOut4.writerow(["File_Name"])

                    count = 0
                    file_count = 0
                    item_len = [0] * len(file_header)
                    for f in os.listdir(input_dir):
                        # print(f)
                        # print(file_count)
                        try:
                            if os.path.getsize(f) < 1000000 * 100:
                                na_count = 0
                                infile_count = 0
                                temp_df = pd.read_csv(f, encoding=enco)
                                
                                # Count file is can be read
                                file_count += 1
                                print(file_count)

                                original_row_num = temp_df.shape[0]
                                temp_df = temp_df.dropna(how="all")

                                # Record how many empty rows were removed
                                drop_row_num = temp_df.shape[0] - original_row_num
                                na_count = na_count + drop_row_num                           
                                # Note: infile count doesn't count header row
                                infile_count = temp_df.shape[0] + infile_count
                                csvOut2.writerow([f, infile_count, na_count])

                                # Add DT_ID & update counts
                                temp_df["DT_ID"] = range(count + 1, count + 1 + temp_df.shape[0])
                                count = temp_df["DT_ID"][temp_df.index[-1]]
                                print(count)

                                # Truncation test function:
                                for i, col_name in enumerate(list(temp_df)):
                                    # print(col_name)
                                    # print(i)
                                    cell_lengths = temp_df[col_name].apply(lambda x: len(str(x)))
                                    max_len = max(cell_lengths)
                                    if max_len > item_len[i]:
                                        item_len[i] = max_len
                                ## Write to csv with "header = False"
                                temp_df.to_csv(outfile, header=False, quoting=csv.QUOTE_ALL, index=False,
                                               line_terminator='\n')
                            
                            # If bigger than certain memory size
                            else:
                                file_count += 1
                                print(file_count)

                                chunk_setting = 100000
                                miss_count = 0
                                infile_count = 0
                                for chunk in pd.read_csv(f, encoding=enco, chunksize=chunk_setting):
                                    original_row_num = chunk.shape[0]
                                    chunk = chunk.dropna(how="all")

                                    # Record how many empty rows were removed
                                    drop_row_num = chunk.shape[0] - original_row_num
                                    miss_count = miss_count + drop_row_num
                                    infile_count = infile_count + chunk.shape[0]


                                    # Add DT_ID & update counts
                                    chunk["DT_ID"] = range(count + 1, count + 1 + chunk.shape[0])
                                    count = chunk["DT_ID"][chunk.index[-1]]
                                    print(count)

                                    # Truncation test function:
                                    for i, col_name in enumerate(list(chunk)):
                                        # print(col_name)
                                        # print(i)
                                        cell_lengths = chunk[col_name].apply(lambda x: len(str(x)))
                                        max_len = max(cell_lengths)
                                        if max_len > item_len[i]:
                                            item_len[i] = max_len
                                    ## Write to csv with "header = False"
                                    chunk.to_csv(outfile, header=False, quoting=csv.QUOTE_ALL, index=False, line_terminator='\n')

                                csvOut2.writerow([f, infile_count, miss_count])
                        except:
                            file_count += 1
                            print(file_count)
                            print("!!!__An Error File__!!!")
                            print(f)
                            print("!!!!!!!!!!!!")
                            csvOut4.writerow(f)
                    csvOut3.writerow(item_len)

    ## Track the end time
    end_time = datetime.datetime.now()

    ## Track the processed counts
    print("Processed records: " + str(count))
    print("Processed files: " + str(file_count))

    ## Track the running time
    print("Processing time: " + str(end_time - start_time))
