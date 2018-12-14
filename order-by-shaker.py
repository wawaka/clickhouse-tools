#!/usr/bin/env python3

import sys
import time
import fnmatch
import argparse
import itertools
import collections

from pprint import pprint as pp

import clickhouse_driver
from beautifultable import BeautifulTable

# how to use
# 1. analyze table columns ./order-by-shaker.py --analyze --from-db-table db.src_table --to-db-table tmp_db.dst
# 2. sort table ./order-by-shaker.py --sort db.src_table -o tmp_db.prefix:user_id,event_time -p event_time cookies.name cookies.value
# 3. compare tables ./order-by-shaker.py --compare 'tmp_db.prefix*'


def make_column_abbr(name):
    return ''.join(tok[0].upper() for tok in name.replace('.', '_').split('_'))


def hrsize(n):
    return "{:.0f} MB".format(n / 1024 / 1024)


def MB(n):
    return int(n / 1024 / 1024)


def P(n):
    return "{:.1%}".format(n)


class ClickhouseCompressionTester(object):
    def __init__(self, args):
        self.args = args
        self.clickhouse_client = clickhouse_driver.Client('localhost')

    def __del__(self):
        self.clickhouse_client.disconnect()

    def run(self, sql):
        print("executing sql: {}".format(sql))
        return self.clickhouse_client.execute(sql)

    def list_columns(self, dbtable):
        db, table = dbtable.split('.')
        rows = self.run("SELECT name FROM system.columns WHERE database = '{}' AND table = '{}'".format(db, table))
        return sorted([r[0] for r in rows])

    def list_tables(self, db):
        rows = self.run("SHOW TABLES FROM {}".format(db))
        return sorted([r[0] for r in rows])

    def get_info(self, dbtable):
        db, table = dbtable.split('.')
        # rows = self.run("SELECT count(), sum(rows), sum(data_compressed_bytes) FROM system.parts WHERE database = '{}' AND table = '{}' AND active".format(db, table))
        # return {
        #     'parts': rows[0][0],
        #     'rows': rows[0][1],
        #     'size': rows[0][2],
        # }

        r = self.run("SELECT column, count(), sum(rows), sum(column_data_compressed_bytes), sum(column_data_uncompressed_bytes) FROM system.parts_columns WHERE database = '{db}' AND table = '{table}' AND active GROUP BY column".format(db=db, table=table))
        # pp(r)
        parts = 0
        rows = 0
        columns = {}
        table_bytes_comp = 0
        table_bytes_uncomp = 0
        for (col, parts, rows, bytes_comp, bytes_uncomp) in r:
            table_bytes_comp += bytes_comp
            table_bytes_uncomp += bytes_uncomp
            columns[col] = {
                'bytes_comp': bytes_comp,
                'bytes_uncomp': bytes_uncomp,
                'comp_ratio': bytes_comp / bytes_uncomp,
            }
        for k, v in columns.items():
            v['table_ratio_comp'] = v['bytes_comp'] / table_bytes_comp
            v['table_ratio_uncomp'] = v['bytes_uncomp'] / table_bytes_uncomp
        # pp(info)
        # exit(0)
        return {
            'rows': rows,
            'parts': parts,
            'bytes_comp': table_bytes_comp,
            'bytes_uncomp': table_bytes_uncomp,
            'comp_ratio': table_bytes_comp / table_bytes_uncomp,
            'columns': columns,
        }

    def optimize(self, dbtable):
        while True:
            info = self.get_info(dbtable)
            if info['parts'] > 1:
                self.run("OPTIMIZE TABLE {} FINAL".format(dbtable))
                time.sleep(1)
                continue
            if info['parts'] == 1:
                return
            raise Exception("bad parts number")

    # def drop(db, table):
    #     run("DROP TABLE {}.{}".format(db, table))

    # def get_column_index(self, column_name):
    #     return self.columns[column_name]['index']

    def make_indexed_table(self, from_dbtable, new_dbtable, select_columns, index_columns):
        db, _ = new_dbtable.split('.')
        self.run("CREATE DATABASE IF NOT EXISTS {}".format(db))
        # info = self.get_info(db, new_table)
        # assert info['parts'] in [0, 1]
        # if info['parts'] > 0 and info['rows'] != ROWS:
            # drop(db, new_table)

        sql = "CREATE TABLE IF NOT EXISTS {new_dbtable} ENGINE = MergeTree ORDER BY ({index_columns}) AS SELECT {select_columns} FROM {from_dbtable}".format(
            from_dbtable = from_dbtable,
            new_dbtable = new_dbtable,
            index_columns = ', '.join(index_columns),
            select_columns = ', '.join(select_columns) if select_columns else '*',
        )
        if self.args.limit:
            sql += " LIMIT {}".format(self.args.limit)
        self.run(sql)

        self.optimize(new_dbtable)

        return self.get_info(new_dbtable)


    # def test_index_columns(db, table, all_columns, base_index):
    #     if len(base_index) >= STOP_DEPTH:
    #         return
    # 
    #     sizes = []
    #     for i, col in enumerate(all_columns):
    #         if i in base_index:
    #             continue
    #         index = base_index + [i]
    #         size = make_indexed_table(db, table, all_columns, index)
    #         sizes.append((size, index))
    # 
    #     sizes.sort(key=lambda x: x[0])
    # 
    #     for sz, index in sizes[:TOP_TABLES]:
    #         test_index_columns(db, table, all_columns, index)

    def make_dbtable_name(self, dbtable, columns):
        return '{dbtable}_{columns}'.format(
            dbtable = dbtable,
            columns = '_'.join("{index}{abbr}".format(**self.columns[col]) for col in columns),
        )

    def read_columns(self, dbtable):
        columns = {}
        for i, col in enumerate(self.list_columns(dbtable)):
            columns[col] = {
                'index': i,
                'abbr': make_column_abbr(col),
                'name': col,
            }
        return columns

    def do_analyze(self):
        self.original_info = self.get_info(self.args.from_db_table)
        self.columns = self.read_columns(self.args.from_db_table)

        to_db, to_table = self.args.to_db_table.split('.')
        self.run("CREATE DATABASE IF NOT EXISTS {}".format(to_db))
        for col in self.columns:
            self.columns[col]['table_info'] = self.make_indexed_table(
                self.args.from_db_table,
                self.make_dbtable_name(self.args.to_db_table, [col]),
                [col],
                [col],
            )

        min_size = 0

        table = BeautifulTable(max_width=100, default_alignment=BeautifulTable.ALIGN_RIGHT)
        table.row_separator_char = ''
        table.column_headers = ["column", "uncomp", "comp_F", "ratio_F", "comp_T", "ratio_T", "max_profit"]
        table.column_alignments['column'] = BeautifulTable.ALIGN_LEFT
        for col, info in self.columns.items():
            try:
                table.append_row([
                    col,
                    MB(info['table_info']['columns'][col]['bytes_uncomp']),
                    MB(self.original_info['columns'][col]['bytes_comp']),
                    P(self.original_info['columns'][col]['comp_ratio']),
                    MB(info['table_info']['columns'][col]['bytes_comp']),
                    P(info['table_info']['columns'][col]['comp_ratio']),
                    MB(self.original_info['columns'][col]['bytes_comp'] - info['table_info']['columns'][col]['bytes_comp']),
                ])
                min_size += info['table_info']['columns'][col]['bytes_comp']
            except Exception as e:
                print(e)

        table.sort('max_profit')

        print(table)

        print("orginal table uncompressed size: {}".format(hrsize(self.original_info['bytes_uncomp'])))
        print("orginal table compressed size: {}".format(hrsize(self.original_info['bytes_comp'])))
        print("min columns compressed size: {}".format(hrsize(min_size)))

    def do_sort(self):
        assert self.args.sort
        assert self.args.output
        self.columns = self.read_columns(self.args.sort)
        for token in self.args.output:
            try:
                dbtable, columns = token.split(':')
            except ValueError:
                dbtable, columns = token, ''
            index_columns = columns.split(',') if columns else []
            for extra_columns in itertools.permutations(self.args.permutations):
                all_index_columns = index_columns + list(extra_columns)
                new_table = self.make_dbtable_name(dbtable, all_index_columns)
                self.make_indexed_table(self.args.sort, new_table, None, all_index_columns)

    def do_compare(self):
        all_columns = set()
        all_tables = []
        for dbpattern in self.args.compare:
            db, pattern = dbpattern.split('.')
            for table in self.list_tables(db):
                if fnmatch.fnmatchcase(table, pattern):
                    print(table)
                    info = self.get_info(db+'.'+table)
                    all_columns |= set(info['columns'].keys())
                    all_tables.append({'name': db+'.'+table, 'info': info})

        all_tables.sort(key=lambda x: -x['info']['bytes_comp'])
        all_columns = sorted(all_columns)

        beautiful_table = BeautifulTable(max_width=200, default_alignment=BeautifulTable.ALIGN_RIGHT)
        # beautiful_table.width_exceed_policy = BeautifulTable.WEP_WRAP
        beautiful_table.row_separator_char = ''
        beautiful_table.column_headers = ['table', 'TOTAL'] + ["{} / {}".format(i, col) for i, col in enumerate(all_columns)]
        beautiful_table.column_alignments['table'] = BeautifulTable.ALIGN_LEFT

        table0 = all_tables[0]

        row = ['UNCOMPRESSED']
        row.append(MB(table0['info']['bytes_uncomp']))
        for col in all_columns:
            row.append(MB(table0['info']['columns'][col]['bytes_uncomp']))
        beautiful_table.append_row(row)

        for table in all_tables:
            assert table0['info']['columns'][col]['bytes_uncomp'] == table['info']['columns'][col]['bytes_uncomp']
            row = [table['name']]
            row.append("{} / {}".format(
                MB(table['info']['bytes_comp']),
                P(table['info']['comp_ratio']),
            ))
            for col in all_columns:
                row.append("{} / {}".format(
                    MB(table['info']['columns'][col]['bytes_comp']),
                    P(table['info']['columns'][col]['comp_ratio']),
                ))
            beautiful_table.append_row(row)

        print(beautiful_table)


def main(args):
    cct = ClickhouseCompressionTester(args)

    if args.sort:
        return cct.do_sort()
    if args.compare:
        return cct.do_compare()
    if args.analyze:
        return cct.do_analyze()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    # parser.add_argument(
    #     '-m', '--mode',
    #     required = True,
    #     choices = ['analyze', 'compare', 'sort'],
    # )
    parser.add_argument(
        '-a', '--analyze',
        action = 'store_true',
        # required = True,
    )
    parser.add_argument(
        '-f', '--from-db-table',
        # required = True,
    )
    parser.add_argument(
        '-t', '--to-db-table',
        # required = True,
    )
    parser.add_argument(
        '-l', '--limit',
        type = int,
    )
    parser.add_argument(
        '-s', '--sort',
        # nargs = '*',
        # type = lambda i: i.split(','),
    )
    parser.add_argument(
        '-c', '--compare',
        nargs = '*',
        # type = lambda t: t.split(','),
    )
    parser.add_argument(
        '-o', '--output',
        nargs = '*',
        # type = lambda t: t.split(','),
    )
    parser.add_argument(
        '-p', '--permutations',
        default = [],
        nargs = '*',
        # type = lambda t: t.split(','),
    )

    args = parser.parse_args()
    print("args: {}".format(args))
    main(args)
