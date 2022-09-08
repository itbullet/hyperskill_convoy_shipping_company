import csv
import json
import pandas as pd
import sqlite3
from lxml import etree


def input_file():
    filename_ = input('Input file name\n').strip()
    name_, extension_ = filename_.split('.')
    checked_ = False
    if "[CHECKED]" in name_:
        checked_ = True
        name_ = name_[:-9]
    return name_, extension_, checked_


def file_convert(filename_, csv_file_name_):
    my_df_ = pd.read_excel(filename_, sheet_name='Vehicles', dtype=str)
    my_df_.to_csv(csv_file_name_, index=None)
    # print(my_df_.columns.ravel())
    # print(my_df_.columns.values)
    # print(my_df_.columns.values.tolist())
    # print(my_df_.shape)
    rows_number = my_df_.shape[0]
    return rows_number


def cell_correction(val_):
    # using regex
    # import re
    # correct_val = re.sub("[^a-zA-Z0-9]+", "",val_)

    # using filter
    # numeric_answer = filter(str.isdigit, val_)
    # correct_val = "".join(numeric_answer)

    # for float
    # float(''.join(c for c in x if (c.isdigit() or c =='.'))

    correct_val = int(''.join(c for c in val_ if c.isdigit()))
    return correct_val


def data_correction(csv_file_name_):
    f_name, extension = csv_file_name_.split('.')
    csv_file_checked_name_ = f'{f_name}[CHECKED].{extension}'
    with open(csv_file_name_, 'r', encoding='utf-8') as file, open(csv_file_checked_name_, 'w',
                                                                   encoding='utf-8') as w_file:
        file_reader = csv.DictReader(file, delimiter=',')
        count_ = 0
        for line in file_reader:
            if count_ == 0:
                names = list(line.keys())
                file_writer = csv.DictWriter(w_file, delimiter=',', lineterminator='\n', fieldnames=names)
                file_writer.writeheader()
            for key, val in line.items():
                if val.isnumeric():
                    line[key] = int(val)
                else:
                    line[key] = cell_correction(val)
                    count_ += 1
            file_writer.writerow(line)
    return csv_file_checked_name_, count_


def info_line(count_, file_name_, obj_, action_):
    ln = f'{obj_} was'
    if count_ != 1:
        ln = f'{obj_}s were'
    return f'{count_} {ln} {action_} to {file_name_}'


def db_connect(db_name_):
    full_db_name_ = f'{db_name_}.s3db'
    conn_ = sqlite3.connect(full_db_name_)
    return conn_, full_db_name_


def db_create(conn_, names_):
    cursor_name_ = conn_.cursor()
    sql_query_as_string = f'''CREATE TABLE convoy (
    {names_[0]} INT PRIMARY KEY,
    {names_[1]} INT NOT NULL,
    {names_[2]} INT NOT NULL,
    {names_[3]} INT NOT NULL,
    score INT NOT NULL
    );'''
    cursor_name_.execute(sql_query_as_string)
    return cursor_name_


def calculate_score(values_):
    score_ = 0
    engine_capacity = int(values_[1])
    fuel_consumption = int(values_[2])
    truck_capacity = int(values_[3])
    avg_route_length = 450
    number_of_pitstops = avg_route_length // (engine_capacity * 100 / fuel_consumption)
    fuel_consumed = avg_route_length * (fuel_consumption / 100)
    if number_of_pitstops == 1:
        score_ += 1
    elif number_of_pitstops == 0:
        score_ += 2

    if fuel_consumed <= 230:
        score_ += 2
    else:
        score_ += 1

    if truck_capacity >= 20:
        score_ += 2

    return score_


def db_write(cursor_name_, data_):
    columns = list(data_.keys())
    values = list(data_.values())
    score = calculate_score(values)
    sql_query_as_string = f'''INSERT INTO convoy ({columns[0]}, {columns[1]}, {columns[2]}, {columns[3]}, score)
    VALUES ({values[0]}, {values[1]}, {values[2]}, {values[3]}, {score});
    '''
    cursor_name_.execute(sql_query_as_string)


def db_read(conn_, table_name_, clause):
    sql_query_as_string = f"SELECT vehicle_id, engine_capacity, fuel_consumption, maximum_load FROM {table_name_} {clause};"
    result = conn_.execute(sql_query_as_string)
    columns = list(map(lambda x: x[0], result.description))
    # columns_ = [description[0] for description in result.description]
    all_rows = result.fetchall()
    return columns, all_rows


def db_close(conn_):
    conn_.commit()
    conn_.close()


def db_main(f_name):
    conn, full_db_name = db_connect(f_name)
    with open(f"{f_name}[CHECKED].csv", 'r', encoding='utf-8') as file:
        file_reader = csv.DictReader(file, delimiter=',')
        db_records_ = 0
        # names = ['vehicle_id', 'engine_capacity', 'fuel_consumption', 'maximum_load']
        for line in file_reader:
            if db_records_ == 0:
                names = list(line.keys())
                cursor_name = db_create(conn, names)
            db_write(cursor_name, line)
            db_records_ += 1
        db_close(conn)
    return full_db_name, db_records_


def json_create(f_name):
    table_name = 'convoy'
    conn, full_db_name = db_connect(f_name)
    clause = f"where score > 3"
    db_column_names, db_rows = db_read(conn, table_name, clause)
    db_close(conn)
    json_data = []
    count_vehicles_ = 0
    for db_row in db_rows:
        json_data.append(dict(zip(db_column_names, db_row)))
        count_vehicles_ += 1
    json_dict = {f'{table_name}': json_data}
    json_file_name_ = f'{f_name}.json'
    with open(json_file_name_, 'w') as json_file:
        json.dump(json_dict, json_file, indent=4)
    return json_file_name_, count_vehicles_


def xml_create(f_name):
    table_name = 'convoy'
    conn, full_db_name = db_connect(f_name)
    clause = f"where score <= 3"
    db_column_names, db_rows = db_read(conn, table_name, clause)
    db_close(conn)
    xml_data = f"<{table_name}>"
    count_xml_vehicles_ = 0
    for db_row in db_rows:
        xml_data += f"<vehicle>"
        for element, data in zip(db_column_names, db_row):
            xml_data += f"<{element}>{data}</{element}>"
        xml_data += f"</vehicle>"
        count_xml_vehicles_ += 1
    xml_data += f"</{table_name}>"
    xml_file_name_ = f"{f_name}.xml"
    root = etree.fromstring(xml_data)
    tree = etree.ElementTree(root)
    # etree.dump(root)
    tree.write(xml_file_name_, pretty_print=True, method="c14n")
    return xml_file_name_, count_xml_vehicles_


def main():
    name, extension, checked = input_file()
    csv_file_name = f"{name}.csv"
    info_str = ''
    if extension.lower() == 'xlsx':
        count_rows = file_convert(f'{name}.{extension}', csv_file_name)
        csv_file_checked_name, count_checked_rows = data_correction(csv_file_name)
        db_name, db_records = db_main(name)
        json_file_name, count_vehicles = json_create(name)
        xml_file_name, count_xml_vehicles = xml_create(name)
        info_str += f"{info_line(count_rows, csv_file_name, 'line', 'added')}\n"
        info_str += f"{info_line(count_checked_rows, csv_file_checked_name, 'cell', 'corrected')}\n"
        info_str += f"{info_line(db_records, db_name, 'record', 'inserted')}\n"
        info_str += f"{info_line(count_vehicles, json_file_name, 'vehicle', 'saved')}\n"
        info_str += f"{info_line(count_xml_vehicles, xml_file_name, 'vehicle', 'saved')}"
    elif extension.lower() == 'csv' and checked is False:
        csv_file_checked_name, count_checked_rows = data_correction(csv_file_name)
        db_name, db_records = db_main(name)
        json_file_name, count_vehicles = json_create(name)
        xml_file_name, count_xml_vehicles = xml_create(name)
        info_str += f"{info_line(count_checked_rows, csv_file_checked_name, 'cell', 'corrected')}\n"
        info_str += f"{info_line(db_records, db_name, 'record', 'inserted')}\n"
        info_str += f"{info_line(count_vehicles, json_file_name, 'vehicle', 'saved')}\n"
        info_str += f"{info_line(count_xml_vehicles, xml_file_name, 'vehicle', 'saved')}"
    elif checked:
        db_name, db_records = db_main(name)
        json_file_name, count_vehicles = json_create(name)
        xml_file_name, count_xml_vehicles = xml_create(name)
        info_str += f"{info_line(db_records, db_name, 'record', 'inserted')}\n"
        info_str += f"{info_line(count_vehicles, json_file_name, 'vehicle', 'saved')}\n"
        info_str += f"{info_line(count_xml_vehicles, xml_file_name, 'vehicle', 'saved')}"
    elif extension.lower() == 's3db':
        json_file_name, count_vehicles = json_create(name)
        xml_file_name, count_xml_vehicles = xml_create(name)
        info_str += f"{info_line(count_vehicles, json_file_name, 'vehicle', 'saved')}\n"
        info_str += f"{info_line(count_xml_vehicles, xml_file_name, 'vehicle', 'saved')}"

    print(info_str)


if __name__ == '__main__':
    main()
