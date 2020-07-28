from json_function import read_from_json, write_to_json, add_to_json
import os
from typing import List, Dict, Any

import db_api
from db_api import DBField, SelectionCriteria

from os import close

MAX_NUM_OF_LINES_IN_FILE = 1000


def delete_table_from_db(table_name):
    db_data = read_from_json("db_files/db.json")
    num_of_files = db_data[table_name]['num of files']
    del (db_data[table_name])
    write_to_json("db_files/db.json", db_data)
    return num_of_files


def delete_files_of_table(table_name, num):
    for i in range(num):
        if os.path.exists(f"db_files/{table_name}_{i + 1}"):
            os.remove(f"db_files/{table_name}_{i + 1}")


def convert_from_DBfield_to_dict(fields):
    return [{field.name: field.type.__name__} for field in fields]


def convert_from_dict_to_DBfield(fields):
    pass


class DBTable:

    def __init__(self, name, fields, key_field_name):
        self.name = name
        self.fields = fields
        self.key_field_name = key_field_name

    def count(self) -> int:
        db_data = read_from_json("db_files/db.json")
        return db_data[self.name]['num of lines']

    def insert_record(self, values: Dict[str, Any]) -> None:
        db_data = read_from_json("db_files/db.json")
        primary_key = db_data[self.name]['primary key']
        record_to_insert = {values[primary_key]: {k: str(v) for k, v in values.items() if k != primary_key}}

        if not db_data[self.name]['num of lines'] % MAX_NUM_OF_LINES_IN_FILE and db_data[self.name][
            'num of lines'] != 0:
            db_data[self.name]['num of files'] += 1
            write_to_json(f"db_files/{self.name}_{db_data[self.name]['num of files']}.json", record_to_insert)
        else:
            add_to_json(f"db_files/{self.name}_{db_data[self.name]['num of files']}.json", record_to_insert)

        db_data[self.name]['num of lines'] += 1
        write_to_json("db_files/db.json", db_data)

    def delete_record(self, key: Any) -> None:
        raise NotImplementedError

    def delete_records(self, criteria: List[SelectionCriteria]) -> None:
        raise NotImplementedError

    def get_record(self, key: Any) -> Dict[str, Any]:
        raise NotImplementedError

    def update_record(self, key: Any, values: Dict[str, Any]) -> None:
        raise NotImplementedError

    def query_table(self, criteria: List[SelectionCriteria]) \
            -> List[Dict[str, Any]]:
        raise NotImplementedError

    def create_index(self, field_to_index: str) -> None:
        raise NotImplementedError


class DataBase:
    # Put here any instance information needed to support the API
    def __init__(self):
        self.num_of_tabels = 0
        write_to_json("db_files/db.json", {'num of tables': 0})

    def create_table(self,
                     table_name: str,
                     fields: List[DBField],
                     key_field_name: str) -> DBTable:
        write_to_json(f"db_files/{table_name}_1.json", {})

        db_data = read_from_json("db_files/db.json")

        db_data['num of tables'] += 1
        db_data[table_name] = \
            {'num of files': 1,
             'num of lines': 0,
             'fields': convert_from_DBfield_to_dict(fields),
             'primary key': key_field_name
             }
        write_to_json("db_files/db.json", db_data)

        return DBTable(table_name, fields, key_field_name)

    def num_tables(self) -> int:
        db_data = read_from_json("db_files/db.json")
        return db_data['num of tables']

    def get_table(self, table_name: str) -> DBTable:
        db_data = read_from_json("db_files/db.json")
        return DBTable(table_name, convert_from_dict_to_DBfield(db_data[table_name]['fields']),
                       db_data[table_name]['primary key'])

    def delete_table(self, table_name: str) -> None:
        num = delete_table_from_db(table_name)
        delete_files_of_table(table_name, num)

    def get_tables_names(self) -> List[Any]:
        db_data = read_from_json("db_files/db.json")
        return [key for key in db_data.keys() if key != 'num of tables']

    def query_multiple_tables(
            self,
            tables: List[str],
            fields_and_values_list: List[List[SelectionCriteria]],
            fields_to_join_by: List[str]
    ) -> List[Dict[str, Any]]:
        raise NotImplementedError
