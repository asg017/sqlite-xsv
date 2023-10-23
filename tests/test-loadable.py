import sqlite3
import unittest
import time
import os

EXT_PATH = "./dist/debug/xsv0"


def connect(ext):
    db = sqlite3.connect(":memory:")

    db.execute("create table base_functions as select name from pragma_function_list")
    db.execute("create table base_modules as select name from pragma_module_list")

    db.enable_load_extension(True)
    db.load_extension(ext)

    db.execute(
        "create temp table loaded_functions as select name from pragma_function_list where name not in (select name from base_functions) order by name"
    )
    db.execute(
        "create temp table loaded_modules as select name from pragma_module_list where name not in (select name from base_modules) order by name"
    )

    db.row_factory = sqlite3.Row
    return db


db = connect(EXT_PATH)


def explain_query_plan(sql):
    return db.execute("explain query plan " + sql).fetchone()["detail"]


def execute_all(sql, args=None):
    if args is None:
        args = []
    results = db.execute(sql, args).fetchall()
    return list(map(lambda x: dict(x), results))


FUNCTIONS = [
    "csv_at",
    "csv_line_number",
    "csv_path",
    "tsv_at",
    "tsv_line_number",
    "tsv_path",
    "xsv_at",
    "xsv_debug",
    "xsv_line_number",
    "xsv_path",
    "xsv_version",
]

MODULES = [
    "csv",
    "csv_fields",
    "csv_reader",
    "csv_rows",
    "tsv",
    "tsv_fields",
    "tsv_reader",
    "tsv_rows",
    "xsv",
    "xsv_fields",
    "xsv_reader",
    "xsv_rows",
]


class TestXsv(unittest.TestCase):
    def test_funcs(self):
        funcs = list(
            map(
                lambda a: a[0],
                db.execute("select name from loaded_functions").fetchall(),
            )
        )
        self.assertEqual(funcs, FUNCTIONS)

    def test_modules(self):
        modules = list(
            map(
                lambda a: a[0], db.execute("select name from loaded_modules").fetchall()
            )
        )
        self.assertEqual(modules, MODULES)

    def test_xsv_version(self):
        self.assertEqual(db.execute("select xsv_version()").fetchone()[0][0], "v")

    def test_xsv_debug(self):
        debug = db.execute("select xsv_debug()").fetchone()[0]
        self.assertEqual(len(debug.splitlines()), 2)

    def test_xsv_at(self):
        self.skipTest("TODO")

    def test_csv_at(self):
        self.skipTest("TODO")

    def test_tsv_at(self):
        self.skipTest("TODO")

    def test_xsv_rows(self):
        # xsv_rows = lambda *args: execute_all("select * from xsv_rows(?, ?)", args)
        # self.assertEqual(xsv_rows("name|age\nalex|10\nbrian|20\ncraig|30", "|"), [])

        self.skipTest("TODO")

    def test_csv_rows(self):
        self.skipTest("TODO")

    def test_tsv_rows(self):
        self.skipTest("TODO")

    def test_xsv_fields(self):
        self.skipTest("TODO")

    def test_csv_fields(self):
        self.skipTest("TODO")

    def test_tsv_fields(self):
        self.skipTest("TODO")

    def test_xsv_line_number(self):
        self.skipTest("TODO")

    def test_csv_line_number(self):
        self.skipTest("TODO")

    def test_tsv_line_number(self):
        self.skipTest("TODO")

    def test_xsv_path(self):
        self.skipTest("TODO")

    def test_csv_path(self):
        self.skipTest("TODO")

    def test_tsv_path(self):
        self.skipTest("TODO")

    def exec_fails_with(self, sql, message, error=sqlite3.OperationalError):
        with self.assertRaisesRegex(error, message):
            execute_all(sql)

    def test_csv_gzip(self):
        db.execute(
            "create virtual table students_gzip using csv(filename='tests/data/students.csv.gz');"
        ).fetchall()
        self.assertEqual(
            execute_all("select rowid, * from students_gzip"),
            [
                {"rowid": 1, "age": "10", "id": "1", "name": "alex", "process": ".9"},
                {"rowid": 2, "age": "20", "id": "2", "name": "brian", "process": ".7"},
                {"rowid": 3, "age": "30", "id": "3", "name": "craig", "process": ".3"},
            ],
        )

    def test_csv_zstd(self):
        db.execute(
            "create virtual table students_zstd using csv(filename='tests/data/students.csv.zst');"
        ).fetchall()
        self.assertEqual(
            execute_all("select rowid, * from students_zstd"),
            [
                {"rowid": 1, "age": "10", "id": "1", "name": "alex", "process": ".9"},
                {"rowid": 2, "age": "20", "id": "2", "name": "brian", "process": ".7"},
                {"rowid": 3, "age": "30", "id": "3", "name": "craig", "process": ".3"},
            ],
        )

    def test_csv_stress(self):
        db.execute('create virtual table "tests/data/students.csv" using csv;')
        self.assertEqual(
            execute_all('select * from "tests/data/students.csv";'),
            [
                {"age": "10", "id": "1", "name": "alex", "process": ".9"},
                {"age": "20", "id": "2", "name": "brian", "process": ".7"},
                {"age": "30", "id": "3", "name": "craig", "process": ".3"},
            ],
        )
        self.assertEqual(
            execute_all(
                'select csv_path(rowid), csv_line_number(rowid), name from "tests/data/students.csv";'
            ),
            [
                {
                    "csv_line_number(rowid)": 2,
                    "csv_path(rowid)": "tests/data/students.csv",
                    "name": "alex",
                },
                {
                    "csv_line_number(rowid)": 3,
                    "csv_path(rowid)": "tests/data/students.csv",
                    "name": "brian",
                },
                {
                    "csv_line_number(rowid)": 4,
                    "csv_path(rowid)": "tests/data/students.csv",
                    "name": "craig",
                },
            ],
        )

        db.execute('create virtual table "tests/data/glob/*.csv" using csv')

        self.assertEqual(
            execute_all(
                'select csv_path(rowid), csv_line_number(rowid), * from "tests/data/glob/*.csv"'
            ),
            [
                {
                    "csv_path(rowid)": "tests/data/glob/a.csv",
                    "csv_line_number(rowid)": 2,
                    "id": "a1",
                    "name": "alex",
                    "age": "10",
                    "process": ".9",
                },
                {
                    "csv_path(rowid)": "tests/data/glob/a.csv",
                    "csv_line_number(rowid)": 3,
                    "id": "a2",
                    "name": "adrian",
                    "age": "20",
                    "process": ".8",
                },
                {
                    "csv_path(rowid)": "tests/data/glob/a.csv",
                    "csv_line_number(rowid)": 4,
                    "id": "a3",
                    "name": "andres",
                    "age": "30",
                    "process": ".7",
                },
                {
                    "csv_path(rowid)": "tests/data/glob/b.csv",
                    "csv_line_number(rowid)": 2,
                    "id": "b1",
                    "name": "brian",
                    "age": "60",
                    "process": ".1",
                },
                {
                    "csv_path(rowid)": "tests/data/glob/b.csv",
                    "csv_line_number(rowid)": 3,
                    "id": "b2",
                    "name": "beto",
                    "age": "50",
                    "process": ".2",
                },
                {
                    "csv_path(rowid)": "tests/data/glob/b.csv",
                    "csv_line_number(rowid)": 4,
                    "id": "b3",
                    "name": "brandy",
                    "age": "40",
                    "process": ".3",
                },
                {
                    "csv_path(rowid)": "tests/data/glob/c.csv",
                    "csv_line_number(rowid)": 2,
                    "id": "c1",
                    "name": "craig",
                    "age": "70",
                    "process": ".4",
                },
                {
                    "csv_path(rowid)": "tests/data/glob/c.csv",
                    "csv_line_number(rowid)": 3,
                    "id": "c2",
                    "name": "catherine",
                    "age": "90",
                    "process": ".5",
                },
                {
                    "csv_path(rowid)": "tests/data/glob/c.csv",
                    "csv_line_number(rowid)": 4,
                    "id": "c3",
                    "name": "coin",
                    "age": "80",
                    "process": ".6",
                },
            ],
        )

        db.execute('create virtual table temp."tests/data/students.csv.gz" using csv;')
        self.assertEqual(
            execute_all('select * from temp."tests/data/students.csv.gz";'),
            [
                {"age": "10", "id": "1", "name": "alex", "process": ".9"},
                {"age": "20", "id": "2", "name": "brian", "process": ".7"},
                {"age": "30", "id": "3", "name": "craig", "process": ".3"},
            ],
        )

    def test_csv_glob_error(self):
        # ok this isn't really an error, but whenever a glob CSV doesn't have a column, how it handles?
        db.execute(
            'create virtual table g_err using csv(filename="tests/data/glob_error/*.csv");'
        )

        self.assertEqual(
            execute_all("select csv_path(rowid), * from g_err;"),
            [
                {
                    "csv_path(rowid)": "tests/data/glob_error/a.csv",
                    "id": "1",
                    "name": "alex",
                    "age": "10",
                    "process": ".9",
                },
                {
                    "csv_path(rowid)": "tests/data/glob_error/a.csv",
                    "id": "2",
                    "name": "adrian",
                    "age": "20",
                    "process": ".8",
                },
                {
                    "csv_path(rowid)": "tests/data/glob_error/a.csv",
                    "id": "3",
                    "name": "andres",
                    "age": "30",
                    "process": ".7",
                },
                {
                    "csv_path(rowid)": "tests/data/glob_error/b.csv",
                    "id": "1",
                    "name": "brian",
                    "age": "60",
                    "process": ".1",
                },
                {
                    "csv_path(rowid)": "tests/data/glob_error/b.csv",
                    "id": "2",
                    "name": "beto",
                    "age": "50",
                    "process": ".2",
                },
                {
                    "csv_path(rowid)": "tests/data/glob_error/b.csv",
                    "id": "3",
                    "name": "brandy",
                    "age": "40",
                    "process": ".3",
                },
                {
                    "csv_path(rowid)": "tests/data/glob_error/c.csv",
                    "id": "1",
                    "name": "craig",
                    "age": "70",
                    "process": None,
                },
                {
                    "csv_path(rowid)": "tests/data/glob_error/c.csv",
                    "id": "2",
                    "name": "catherine",
                    "age": "90",
                    "process": None,
                },
                {
                    "csv_path(rowid)": "tests/data/glob_error/c.csv",
                    "id": "3",
                    "name": "coin",
                    "age": "80",
                    "process": None,
                },
            ],
        )

    def test_xsv(self):
        db.execute(
            "create virtual table students_psv using xsv(filename='tests/data/students.psv', delimiter='|');"
        ).fetchall()
        self.assertEqual(
            execute_all("select rowid, * from students_psv"),
            [
                {"rowid": 1, "age": "10", "id": "1", "name": "alex", "process": ".9"},
                {"rowid": 2, "age": "20", "id": "2", "name": "brian", "process": ".7"},
                {"rowid": 3, "age": "30", "id": "3", "name": "craig", "process": ".3"},
            ],
        )

        # ensure "\t" can be passed in as delimiter
        db.execute(
            "create virtual table students_tsv_with_xsv using xsv(filename='tests/data/students.tsv', delimiter='\t');"
        ).fetchall()
        self.assertEqual(
            execute_all("select rowid, * from students_tsv_with_xsv"),
            [
                {"rowid": 1, "age": "10", "id": "1", "name": "alex", "process": ".9"},
                {"rowid": 2, "age": "20", "id": "2", "name": "brian", "process": ".7"},
                {"rowid": 3, "age": "30", "id": "3", "name": "craig", "process": ".3"},
            ],
        )

        # test with custom column names
        db.execute(
            """create virtual table students_psv_with_column_declarations using xsv(
      filename='tests/data/students.psv',
      delimiter='|',
      id text,
      name text,
      age int,
      process real
      );"""
        ).fetchall()
        self.assertEqual(
            execute_all("select rowid, * from students_psv_with_column_declarations"),
            [
                {"rowid": 1, "age": 10, "id": "1", "name": "alex", "process": 0.9},
                {"rowid": 2, "age": 20, "id": "2", "name": "brian", "process": 0.7},
                {"rowid": 3, "age": 30, "id": "3", "name": "craig", "process": 0.3},
            ],
        )
        db.execute(
            """create virtual table students_psv_with_column_declarations_not_enough using xsv(
      filename='tests/data/students.psv',
      delimiter='|',
      id text,
      name text,
      age int
      );"""
        ).fetchall()
        self.assertEqual(
            execute_all(
                "select rowid, * from students_psv_with_column_declarations_not_enough"
            ),
            [
                {"rowid": 1, "age": 10, "id": "1", "name": "alex"},
                {"rowid": 2, "age": 20, "id": "2", "name": "brian"},
                {"rowid": 3, "age": 30, "id": "3", "name": "craig"},
            ],
        )

    def test_xsv_inferred_name(self):
        db.execute(
            "create virtual table temp.\"tests/data/students.psv\" using xsv(delimiter='|');"
        )
        self.assertEqual(
            execute_all('select * from temp."tests/data/students.psv"'),
            [
                {"age": "10", "id": "1", "name": "alex", "process": ".9"},
                {"age": "20", "id": "2", "name": "brian", "process": ".7"},
                {"age": "30", "id": "3", "name": "craig", "process": ".3"},
            ],
        )

    def test_xsv_headers(self):
        # header work
        db.execute(
            "create virtual table students_no_header using xsv(filename='tests/data/students_no_header.csv', delimiter=',', header='off');"
        ).fetchall()
        self.assertEqual(
            execute_all("select rowid, * from students_no_header"),
            [
                {"rowid": 1, "c1": "11", "c2": "alex", "c3": "10", "c4": ".9"},
                {"rowid": 2, "c1": "12", "c2": "brian", "c3": "20", "c4": ".7"},
                {"rowid": 3, "c1": "13", "c2": "craig", "c3": "30", "c4": ".3"},
            ],
        )

        db.execute(
            "create virtual table students_header_yes using xsv(filename='tests/data/students.csv', delimiter=',', header=yes);"
        ).fetchall()
        self.assertEqual(
            execute_all("select rowid, * from students_header_yes"),
            [
                {"rowid": 1, "id": "1", "name": "alex", "age": "10", "process": ".9"},
                {"rowid": 2, "id": "2", "name": "brian", "age": "20", "process": ".7"},
                {"rowid": 3, "id": "3", "name": "craig", "age": "30", "process": ".3"},
            ],
        )

        db.execute(
            """create virtual table students_no_header_with_cols using xsv(
      filename='tests/data/students_no_header.csv',
      delimiter=',',
      header='off',
      id text,
      name text,
      age int,
      process real
      );"""
        ).fetchall()
        self.assertEqual(
            execute_all("select rowid, * from students_no_header_with_cols"),
            [
                {"rowid": 1, "id": "11", "name": "alex", "age": 10, "process": 0.9},
                {"rowid": 2, "id": "12", "name": "brian", "age": 20, "process": 0.7},
                {"rowid": 3, "id": "13", "name": "craig", "age": 30, "process": 0.3},
            ],
        )

    def test_tsv(self):
        db.execute(
            "create virtual table students_tsv using tsv(filename='tests/data/students.tsv');"
        ).fetchall()
        self.assertEqual(
            execute_all("select rowid, * from students_tsv"),
            [
                {"rowid": 1, "age": "10", "id": "1", "name": "alex", "process": ".9"},
                {"rowid": 2, "age": "20", "id": "2", "name": "brian", "process": ".7"},
                {"rowid": 3, "age": "30", "id": "3", "name": "craig", "process": ".3"},
            ],
        )

    def test_csv(self):
        db.execute(
            "create virtual table students using csv(filename='tests/data/students.csv');"
        ).fetchall()
        self.assertEqual(
            execute_all("select rowid, * from students"),
            [
                {"rowid": 1, "age": "10", "id": "1", "name": "alex", "process": ".9"},
                {"rowid": 2, "age": "20", "id": "2", "name": "brian", "process": ".7"},
                {"rowid": 3, "age": "30", "id": "3", "name": "craig", "process": ".3"},
            ],
        )
        self.assertRegex(
            explain_query_plan("select * from students"),
            "SCAN (TABLE )?students VIRTUAL TABLE INDEX 1:",
        )
        self.assertEqual(
            execute_all(
                "select cid, name, type, hidden from pragma_table_xinfo('students')"
            ),
            [
                {"cid": 0, "name": "id", "type": "", "hidden": 0},
                {"cid": 1, "name": "name", "type": "", "hidden": 0},
                {"cid": 2, "name": "age", "type": "", "hidden": 0},
                {"cid": 3, "name": "process", "type": "", "hidden": 0},
            ],
        )

        # testing when there's not enough columns in a row
        db.execute(
            "create virtual table not_enough_columns using csv(filename='tests/data/not_enough_columns.csv');"
        ).fetchall()
        self.assertEqual(
            execute_all("select * from not_enough_columns limit 1"),
            [
                {"a": "1", "b": "2", "c": "3"},
            ],
        )
        with self.assertRaisesRegex(
            sqlite3.OperationalError,
            "Error while reading next row: CSV error: .* found record with 2 fields, but the previous record has 3 fields",
        ):
            execute_all("select * from not_enough_columns")

        # testing whe there's too many columns in a row
        db.execute(
            "create virtual table too_many_columns using csv(filename='tests/data/too_many_columns.csv');"
        ).fetchall()
        self.assertEqual(
            execute_all("select * from too_many_columns limit 1;"),
            [
                {"a": "1", "b": "2", "c": "3"},
            ],
        )
        with self.assertRaisesRegex(
            sqlite3.OperationalError,
            "Error while reading next row: CSV error: .* found record with 4 fields, but the previous record has 3 fields",
        ):
            execute_all("select * from too_many_columns")

        self.exec_fails_with(
            "create virtual table x using csv();",
            "no filename given. Specify a path to a CSV file to read from with 'filename=\"path.csv\"'",
        )
        self.exec_fails_with(
            "create virtual table x using csv(filename='not_exist.csv');",
            "No matching files found for not_exist.csv",
        )
        self.exec_fails_with(
            "create virtual table x using csv(filename='tests/data/invalid-header.csv');",
            "Error: invalid UTF8 in headers of CSV",
        )
        self.exec_fails_with(
            "create virtual table q using csv(filename);",
            "no filename given. Specify a path to a CSV file to read from with 'filename=\"path.csv\"'",
        )
        self.exec_fails_with(
            "create virtual table q using csv(file=);", "Empty value for key 'file'"
        )
        self.exec_fails_with(
            "create virtual table q using csv(filename=students.csv);",
            "'filename' value must be string, wrap in single or double quotes.",
        )

        # self.exec_fails_with(
        #  "create virtual table q using csv(filename=:not_exist);",
        #  "temp.sqlite_parameters is not defined, can't use sqlite parameters as value"
        # )

        db.execute(
            "create virtual table invalidrow using csv(filename='tests/data/invalid-row.csv');"
        ).fetchall()
        self.exec_fails_with(
            "select * from invalidrow;", "Error: UTF8 error while reading next row"
        )

        # self.exec_fails_with(
        #  "create virtual table x using csv(filename=\"what\");",
        #  "Error: no file extension detected for 'what'"
        # )

    def test_csv_reader(self):
        # now with affinity!
        execute_all(
            "create virtual table students_reader using csv_reader(id integer, name, age integer, progess real);"
        )
        self.assertEqual(
            execute_all(
                "select * from students_reader('tests/data/student_files/a.csv')"
            ),
            [
                {"age": 10, "id": 1, "name": "alex", "progess": 0.9},
                {"age": 20, "id": 2, "name": "adrian", "progess": 0.8},
                {"age": 30, "id": 3, "name": "andres", "progess": 0.7},
            ],
        )
        self.assertEqual(
            execute_all(
                """
      with files as (
        select value as path
        from json_each('["tests/data/student_files/a.csv", "tests/data/student_files/b.csv", "tests/data/student_files/c.csv"]')
      )
      select students.*
      from files
      join students_reader(files.path) as students;
      """
            ),
            [
                {"id": 1, "name": "alex", "age": 10, "progess": 0.9},
                {"id": 2, "name": "adrian", "age": 20, "progess": 0.8},
                {"id": 3, "name": "andres", "age": 30, "progess": 0.7},
                {"id": 1, "name": "brian", "age": 60, "progess": 0.1},
                {"id": 2, "name": "beto", "age": 50, "progess": 0.2},
                {"id": 3, "name": "brandy", "age": 40, "progess": 0.3},
                {"id": 1, "name": "craig", "age": 70, "progess": 0.4},
                {"id": 2, "name": "catherine", "age": 90, "progess": 0.5},
                {"id": 3, "name": "coin", "age": 80, "progess": 0.6},
            ],
        )

        # TODO  skipping this bc on gh actions ubuntu, the 'type' fields are lowercase.
        #       why? it's 'integer' instead of 'INTEGER', but not on my droplet, wild
        if False:
            self.assertEqual(
                execute_all("select * from pragma_table_xinfo('students_reader');"),
                [
                    {
                        "cid": 0,
                        "name": "_source",
                        "type": "",
                        "notnull": 0,
                        "dflt_value": None,
                        "pk": 0,
                        "hidden": 1,
                    },
                    # TODO does "integer primary key" ever make sense?
                    {
                        "cid": 1,
                        "name": "id",
                        "type": "INTEGER",
                        "notnull": 0,
                        "dflt_value": None,
                        "pk": 0,
                        "hidden": 0,
                    },
                    {
                        "cid": 2,
                        "name": "name",
                        "type": "",
                        "notnull": 0,
                        "dflt_value": None,
                        "pk": 0,
                        "hidden": 0,
                    },
                    {
                        "cid": 3,
                        "name": "age",
                        "type": "INTEGER",
                        "notnull": 0,
                        "dflt_value": None,
                        "pk": 0,
                        "hidden": 0,
                    },
                    {
                        "cid": 4,
                        "name": "progess",
                        "type": "REAL",
                        "notnull": 0,
                        "dflt_value": None,
                        "pk": 0,
                        "hidden": 0,
                    },
                ],
            )

    def test_tsv_reader(self):
        execute_all(
            "create virtual table students_tsv_reader using tsv_reader(id integer primary key, name text, age integer, progess real);"
        )
        self.assertEqual(
            execute_all(
                "select * from students_tsv_reader('tests/data/student_files/a.tsv')"
            ),
            [
                {"age": 10, "id": 1, "name": "alex", "progess": 0.9},
                {"age": 20, "id": 2, "name": "adrian", "progess": 0.8},
                {"age": 30, "id": 3, "name": "andres", "progess": 0.7},
            ],
        )

    def test_xsv_reader(self):
        execute_all(
            "create virtual table students_psv_reader using xsv_reader(delimiter='|', id integer primary key, name text, age integer, progess real);"
        )
        self.assertEqual(
            execute_all(
                "select * from students_psv_reader('tests/data/student_files/a.psv')"
            ),
            [
                {"age": 10, "id": 1, "name": "alex", "progess": 0.9},
                {"age": 20, "id": 2, "name": "adrian", "progess": 0.8},
                {"age": 30, "id": 3, "name": "andres", "progess": 0.7},
            ],
        )

    def test_xsv_reader_header(self):
        execute_all(
            "create virtual table xsv_reader_no_header using xsv_reader(delimiter=',', header=no, id, name text, age integer, progess real);"
        )
        self.assertEqual(
            execute_all(
                "select rowid, * from xsv_reader_no_header('tests/data/students_no_header.csv')"
            ),
            [
                {"rowid": 1, "id": "11", "name": "alex", "age": 10, "progess": 0.9},
                {"rowid": 2, "id": "12", "name": "brian", "age": 20, "progess": 0.7},
                {"rowid": 3, "id": "13", "name": "craig", "age": 30, "progess": 0.3},
            ],
        )


class TestCoverage(unittest.TestCase):
    def test_coverage(self):
        test_methods = [method for method in dir(TestXsv) if method.startswith("test_")]
        funcs_with_tests = set([x.replace("test_", "") for x in test_methods])

        for func in FUNCTIONS:
            self.assertTrue(
                func in funcs_with_tests,
                f"{func} does not have corresponding test in {funcs_with_tests}",
            )

        for module in MODULES:
            self.assertTrue(
                module in funcs_with_tests,
                f"{module} does not have corresponding test in {funcs_with_tests}",
            )


if __name__ == "__main__":
    unittest.main()
