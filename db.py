import operator

from json_function import read_from_json, write_to_json, add_to_json
import os
from typing import List, Dict, Any
from bplustree import BPlusTree
import db_api
from db_api import DBField, SelectionCriteria

from os import close

MAX_NUM_OF_LINES_IN_FILE = 1000

ops = {"=": operator.eq, "<": operator.lt, "<=": operator.le, ">": operator.gt, ">=": operator.ge, "!=": operator.ne}


def end_place_in_file(data, name):
    return not data[name]['num of lines'] % MAX_NUM_OF_LINES_IN_FILE


# def update_index(indexes, path, values):
#     for index in indexes:


def delete_table_from_db(table_name):
    db_data = read_from_json("db_files/db.json")
    num_of_files = db_data[table_name]['num of files']
    del (db_data[table_name])
    db_data['num of tables'] -= 1
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


def update_data(table_name, num, file_data,  file_num):
    db_data = read_from_json("db_files/db.json")
    db_data[table_name]['num of lines'] -= num
    write_to_json(f"db_files/{table_name}_{file_num + 1}.json", file_data)
    write_to_json("db_files/db.json", db_data)


def delete_relevant_keys(keys_to_delete, file_data, table_name, file_num):
    count = 0

    for k in keys_to_delete:
        del file_data[k]
        count += 1

    update_data(table_name, count, file_data, file_num)
    #return count


def treats_relevant(num, table_name, criteria, primary_key, delete):
    relevants = []

    for file_num in range(num):
        file_data = read_from_json(f"db_files/{table_name}_{file_num + 1}.json")

        for k, v in file_data.items():
            flag = 0

            for c in criteria:

                if c.field_name == primary_key:

                    if not ops[c.operator](int(k), int(c.value)):
                        flag = 1
                        break

                else:

                    if not ops[c.operator](v[c.field_name], c.value):
                        flag = 1
                        break

            if not flag:
                if not delete:
                    relevants.append(v)

                else:
                    relevants.append(k)

        if delete:
            delete_relevant_keys(relevants, file_data, table_name, file_num)
            # update_lines(table_name, num_of_deletes, file_data,  file_num)

    return relevants


def get_primary_key(data, table_name):
    return data[table_name]['primary key']


def get_num_of_files(data, table_name):
    return data[table_name]['num of files']


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
        flag = None
        primary_key = db_data[self.name]['primary key']
        try:
            flag = self.get_record(values[primary_key])
        except:
            db_data = read_from_json("db_files/db.json")
            primary_key = get_primary_key(db_data, self.name)
            record_to_insert = {values[primary_key]: {k: str(v) for k, v in values.items() if k != primary_key}}

            if end_place_in_file(db_data, self.name) and db_data[self.name]['num of lines'] != 0:
                db_data[self.name]['num of files'] += 1
                write_to_json(f"db_files/{self.name}_{db_data[self.name]['num of files']}.json", record_to_insert)
            else:
                add_to_json(f"db_files/{self.name}_{db_data[self.name]['num of files']}.json", record_to_insert)

        if flag is not None:
            raise ValueError

        db_data[self.name]['num of lines'] += 1
        write_to_json("db_files/db.json", db_data)

        # update_index(db_data[self.name]['indexes'], path, values)

    def delete_record(self, key: Any) -> None:
        db_data = read_from_json("db_files/db.json")
        num_of_files = db_data[self.name]['num of files']
        flag = False
        for file_num in range(num_of_files):
            file_data = read_from_json(f"db_files/{self.name}_{file_num + 1}.json")
            for k in file_data.keys():
                if k == str(key):
                    flag = True
                    del_key = k
                    break
            if flag:
                del file_data[del_key]
                write_to_json(f"db_files/{self.name}_{file_num + 1}.json", file_data)
        if not flag:
            raise ValueError
        db_data[self.name]['num of lines'] -= 1
        write_to_json("db_files/db.json", db_data)

    def delete_records(self, criteria: List[SelectionCriteria]) -> None:
        db_data = read_from_json("db_files/db.json")
        num = get_num_of_files(db_data, self.name)
        pk = get_primary_key(db_data, self.name)
        treats_relevant(num, self.name, criteria, pk, True)

    def get_record(self, key: Any) -> Dict[str, Any]:
        db_data = read_from_json("db_files/db.json")
        num_of_files = db_data[self.name]['num of files']
        for file_num in range(num_of_files):
            file_data = read_from_json(f"db_files/{self.name}_{file_num + 1}.json")
            for k in file_data.keys():
                if k == str(key):
                    return file_data[k]
            else:
                raise ValueError

    def update_record(self, key: Any, values: Dict[str, Any]) -> None:
        db_data = read_from_json("db_files/db.json")
        num_of_files = db_data[self.name]['num of files']
        for file_num in range(num_of_files):
            file_data = read_from_json(f"db_files/{self.name}_{file_num + 1}.json")
            for k in file_data.keys():
                if k == str(key):
                    for field in values.keys():
                        file_data[k][field] = values[field]
                    write_to_json(f"db_files/{self.name}_{file_num + 1}.json", file_data)
                    return
        raise ValueError

    def query_table(self, criteria: List[SelectionCriteria]) \
            -> List[Dict[str, Any]]:
        db_data = read_from_json("db_files/db.json")
        num = get_num_of_files(db_data, self.name)
        pk = get_primary_key(db_data, self.name)

        return treats_relevant(num, self.name, criteria, pk, False)


    def create_index(self, field_to_index: str) -> None:
        index_tree = BPlusTree(f'db_files/{self.name}_order_by_{field_to_index}.db', order=50)
        db_data = read_from_json("db_files/db.json")
        if field_to_index in db_data[self.name]['indexes']:
            return
        num_of_files = db_data[self.name]['num of files']
        primary_key = db_data[self.name]['primary key']

        if primary_key == field_to_index:

            for file_num in range(num_of_files):

                path = f"db_files/{self.name}_{file_num + 1}.json"
                file_data = read_from_json(path)

                for k in file_data.keys():
                    print(k)
                    index_tree[k] = path
        else:
            for file_num in range(num_of_files):
                path = f"db_files/{self.name}_{file_num + 1}.json"
                file_data = read_from_json(path)

                for v in file_data.values():
                    print(v)
                    index_tree[v[field_to_index]] = path
        index_tree.close()
        db_data[self.name]['indexes'] += field_to_index


class DataBase:
    # Put here any instance information needed to support the API
    def __init__(self):
        if not os.path.isfile("db_files/db.json"):
            write_to_json("db_files/db.json", {'num of tables': 0})

    def create_table(self,
                     table_name: str,
                     fields: List[DBField],
                     key_field_name: str) -> DBTable:
        if os.path.isfile(f"db_files/{table_name}_1.json"):
            raise ValueError
        if key_field_name not in [field.name for field in fields]:
            raise ValueError
        db_data = read_from_json("db_files/db.json")
        if table_name in db_data.keys():
            raise ValueError

        write_to_json(f"db_files/{table_name}_1.json", {})

        db_data['num of tables'] += 1
        db_data[table_name] = \
            {'num of files': 1,
             'num of lines': 0,
             'fields': convert_from_DBfield_to_dict(fields),
             'primary key': key_field_name,
             'indexes': []
             }
        write_to_json("db_files/db.json", db_data)

        table = DBTable(table_name, fields, key_field_name)
        table.create_index(key_field_name)

        return table

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
