#include <cassert>
#include <iostream>
#include "rocksdb/db.h"
#include <cstdlib>
#include <cstdio>
#include <cstring>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

using namespace std;
using namespace rocksdb;

int main(int argc, char *argv[]) {
	/* Variable declarations and some setup */
	assert(argc > 0);

	Options options;
    options.create_if_missing = false;
    DB* db;
    std::vector<ColumnFamilyHandle*> handles;
    std::vector<ColumnFamilyDescriptor> column_families = {
        ColumnFamilyDescriptor(kDefaultColumnFamilyName, ColumnFamilyOptions()),
        ColumnFamilyDescriptor("two", ColumnFamilyOptions())
    };

	// int fd, pos;
	// int retreived_rows = 0;
	// int row_present[10] = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
	char db_path[10000];

	strcpy(db_path, argv[1]);
	strcat(db_path, "transaction_testdb_2854226679369055161");

	Status s = DB::Open(options, db_path, column_families, &handles, &db);
	assert(s.ok());

	/* Read the database, and verify *atomicity*, i.e., whether the retreived
	 * key-value pairs are the same as those inserted during the workload.
	 * (workload.cc and init.cc inserts unique strings corresponding to the numbers
	 * 0 to 9, as key-value pairs.) 
	 *
	 * Also record the total number of key-value pairs retreived, in the
	 * "retreived_rows" variable. 
	 *
	 * Also record which exact key-value pairs (corresponding to which numbers
	 * between 0 and 9) are retreived, in the row_present array.*/

	// Check data integrity for all keys.
	std::string large_value(400, ' ');
    std::vector<std::pair<std::string, std::string>> expected_data = {
        {"foo1", "bar1"},
        {"foo2", "bar2"},
        // Note: Depending on corruption handling, foo3 might not be reliably checked.
		{"foo3", large_value},
        {"foo4", "bar4"}
        // Add {"foo5", large_value} if write_after_recovery was true.
    };

	for (const auto& kv : expected_data) {
        std::string key = kv.first;
        std::string expected_value = kv.second;
        std::string value;
        s = db->Get(ReadOptions(), key, &value);
        assert(s.ok() && value == expected_value);
    }

    // Verify the transaction "xid" has been committed.
    std::string value;
    s = db->Get(ReadOptions(), "foo-prepare", &value);
    assert(s.ok() && value == "bar-prepare");

    // Clean up.
    for (auto* handle : handles) {
        delete handle;
    }
    delete db;

}
