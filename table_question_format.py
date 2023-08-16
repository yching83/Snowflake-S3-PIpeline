# Importing pandas package

import csv
import pandas as pd
import sys 
import json

#csvfilename = "result10.csv"
# df = pd.read_csv(df)

def read_dataframe (a_dataframe):
    df = a_dataframe
    datalist = df.values.tolist()

    print(f"datalist: {datalist[0]}") 

    total = []
    items = []
    headers = []

    table_dict = {}

    totalcost_row = {}
    totalcost_max = {}
    totalcost = {}
    rowvalue_max = {}

    colheaders = []
    headers.insert(0, "")
    colheaders.insert(0, "")

    maxcols = 0

    for line in datalist:
        if ':' in line[0]:
            row = line[0].split(":")[0][1:]
            col = line[0].split(":")[1][1:]
            if int(col) > maxcols:
                maxcols = int(col)
            elem = str(line[1]).strip()
            folder = str(line[2]).strip()
            if folder not in rowvalue_max.keys():
                rowvalue_max[folder] = 0
            if int(row) > rowvalue_max[folder]:
                rowvalue_max[folder] = int(row)
            if row == "0":
                if elem not in headers:
                    headers.insert(int(col)+1, elem)
                    colheaders.insert(int(col)+1, "c"+col)
            else:
                print(f"Row: {row} / Col: {col} / Elem: {elem} / Folder {folder}")
                if folder not in table_dict.keys():
                    table_dict[folder] = {}
                if row not in table_dict[folder].keys():
                    table_dict[folder][row] = {}
                if "item" not in table_dict[folder][row].keys():
                    table_dict[folder][row]["item"] = ""
                if "cost" not in table_dict[folder][row].keys():
                    table_dict[folder][row]["cost"] = ""
                if "Total Cost" in elem:
                    if folder not in totalcost_row.keys():
                        totalcost_row[folder] = {}
                    totalcost_row[folder] = row
                else:
                    if folder in totalcost_row.keys():
                        if totalcost_row[folder] == row:
                            # need to check if I have a previous row with the total cost value
                            if folder not in totalcost_max.keys():
                                if row in table_dict[folder].keys():
                                    if "cost" in table_dict[folder][row].keys():
                                        if col == "1":
                                            print(f"Elem: {elem}")
                                            value = table_dict[folder][row]["cost"]
                                            print(f"value: {value} / len: {len(value)}")
                                            if len(value) == 0:
                                                value = 0
                                            if float(elem) > float(value):
                                                totalcost_max[folder] = elem
                                            else:
                                                totalcost_max[folder] = value
                                        else:
                                            totalcost_max[folder] = 0
                                    else:
                                        if col == "1":
                                            totalcost_max[folder] = elem
                                        else:
                                            totalcost_max[folder] = 0
                                else:
                                    if col == "1":
                                        totalcost_max[folder] = elem
                                    else:
                                        totalcost_max[folder] = 0
                            else:
                                if col == "1":
                                    if float(elem) > float(totalcost_max[folder]):
                                        totalcost_max[folder] = elem

                if col == "0":
                    table_dict[folder][row]["item"] = elem
                else:
                    table_dict[folder][row]["cost"] = elem
        else:
            pass

    # # fix Total Cost:
    # for keyfolder, value in table_dict.items():
    #     print(f"keyfolder: {keyfolder}")
    #     if keyfolder not in totalcost_max.keys():
    #         totalcost_max[keyfolder] = 0
    #     table_dict[keyfolder][totalcost_row[keyfolder]]["cost"] = str(totalcost_max[keyfolder])

    print(json.dumps(table_dict, indent=2))
    result_csv = []
    colheaders.insert(maxcols + 2, "c" + str(maxcols+1))
    headers.insert(maxcols + 2, "Path")

    newheader = []
    for i, (colname, headername) in enumerate(zip(colheaders, headers)):
        if i == 0:
            newheader.append("")
        else:
            if headername == 'Path':
                newheader.append("FOLDER_DIRECTORY")
            else:
                #newheader.append(headername + "-" + colname)
                newheader.append(headername)

    # result_csv.append(colheaders)
    # result_csv.append(headers)

    result_csv.append(newheader)


    for keyfolder, value in table_dict.items():
        for idx in range(rowvalue_max[keyfolder] + 1):
            if str(idx) in table_dict[keyfolder].keys():
                # print(f"idx: {idx}")
                line = ["r" + str(idx), table_dict[keyfolder][str(idx)]['item'] , table_dict[keyfolder][str(idx)]['cost'], keyfolder]
                
                # print(f"{line}")
                result_csv.append(line)

    columns = ['', 'Item', 'Cost', 'FOLDER_DIRECTORY']
    result_set =  pd.DataFrame(result_csv, columns=columns)
    result_set.reset_index(drop = True)
    result_set2 = result_set[["Item", "Cost","FOLDER_DIRECTORY"]]
    result_set2 = result_set2.reset_index(drop = True)
    return result_set2
