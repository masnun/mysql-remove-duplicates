import MySQLdb
import string

HOST = "localhost"
USERNAME = "root"
PWD = "s3Cret!"
DATABASE = "test"

FIELDS = ('user_id', 'user_name', 'state', 'age', 'gender')
TABLENAME = 'user_data'
PRIMARY_KEY = 'id'


class DuplicateFinder(object):
    def __init__(self):
        self.connection = MySQLdb.connect(HOST, USERNAME, PWD, DATABASE).cursor(MySQLdb.cursors.DictCursor)
        self.fields = FIELDS
        self.table = TABLENAME
        self.pk = PRIMARY_KEY
        self.processed = []

    def search_and_destroy(self):
        rows = finder.run_query("SELECT * FROM " + str(self.table), True)
        print "Processing %d entries" % len(rows)
        print "----------" * 3
        for x in rows:
            print "Primary Key: %s " % str(x[finder.pk])
            is_processed = self.is_processed(x)
            if not is_processed:
                duplicates = self.find_duplicates(x)
                if duplicates:
                    print "Found %d duplicates" % len(duplicates)
                    self.remove_duplicates(duplicates)
            else:
                print "Primary Key: %s already processed before " % str(x[finder.pk])

            print "----------" * 3


    def find_duplicates(self, row):
        where_dict = {}
        for field in self.fields:
            value = row.get(field, None)
            if value:
                where_dict[field] = value

        where_clause = []
        for k, v in where_dict.iteritems():
            where_clause.append(str(k) + "= '" + str(v) + "'")
        where = " AND ".join(where_clause)

        if len(where_clause) > 0:
            query = "SELECT " + self.pk + " FROM " + self.table + " WHERE " + where + " AND  " \
                    + self.pk + " != '" + str(row[self.pk]) + "'"

            dups = self.run_query(query, True)

            return [str(x[self.pk]) for x in dups]
        else:
            return False

    def remove_duplicates(self, pk_collection):
        for pk in pk_collection:
            self.processed.append(pk)

        print "Removing Primary Keys: " + string.join(pk_collection, ",")
        query = "DELETE FROM " + self.table + " WHERE " + self.pk \
                + " IN (" + string.join(pk_collection, ",") + ")"

        return self.run_query(query)

    def is_processed(self, row):
        return str(row[self.pk]) in self.processed

    def run_query(self, sql, fetch_all=False):
        success = self.connection.execute(sql)
        if not fetch_all:
            return success
        else:
            return self.connection.fetchall()


finder = DuplicateFinder()
finder.search_and_destroy()

