import unittest
import sqlite3
import sqlite_xsv

class TestSqlitexsvPython(unittest.TestCase):
  def test_path(self):
    db = sqlite3.connect(':memory:')
    db.enable_load_extension(True)

    self.assertEqual(type(sqlite_xsv.loadable_path()), str)
    
    sqlite_xsv.load(db)
    version, = db.execute('select xsv_version()').fetchone()
    self.assertEqual(version[0], "v")

if __name__ == '__main__':
    unittest.main()