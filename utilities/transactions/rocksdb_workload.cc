#include "transaction_test.h"

using namespace std;
using namespace rocksdb;

class MyTransactionTestBase {
 public:
  TransactionDB* db;
  FaultInjectionTestEnv* env;
  std::string dbname;
  Options options;

  TransactionDBOptions txn_db_options;
  bool use_stackable_db_;
  bool keep_db_local_ = true; // Used to keep the db temporary files after the test is done

  MyTransactionTestBase(bool use_stackable_db, bool two_write_queue,
                      TxnDBWritePolicy write_policy,
                      WriteOrdering write_ordering)
      : db(nullptr), env(nullptr), use_stackable_db_(use_stackable_db) {
    options.create_if_missing = true;
    options.max_write_buffer_number = 2;
    options.write_buffer_size = 4 * 1024;
    options.unordered_write = write_ordering == kUnorderedWrite;
    options.level0_file_num_compaction_trigger = 2;
    options.merge_operator = MergeOperators::CreateFromStringId("stringappend");
    env = new FaultInjectionTestEnv(Env::Default());
    options.env = env;
    options.two_write_queues = two_write_queue;
    // use perThreadDBPath to avoid conflict with other tests
    dbname = test::PerThreadDBPath("/home/jiexiao/squint/copy_bug1/pmcc/squint_test_dir", "transaction_testdb");

    DestroyDB(dbname, options);
    txn_db_options.transaction_lock_timeout = 0;
    txn_db_options.default_lock_timeout = 0;
    txn_db_options.write_policy = write_policy;
    txn_db_options.rollback_merge_operands = true;
    // This will stress write unprepared, by forcing write batch flush on every
    // write.
    txn_db_options.default_write_batch_flush_threshold = 1;
    // Write unprepared requires all transactions to be named. This setting
    // autogenerates the name so that existing tests can pass.
    // txn_db_options.autogenerate_name = true;
    Status s;
    if (use_stackable_db == false) {
      s = TransactionDB::Open(options, txn_db_options, dbname, &db);
    } else {
      s = OpenWithStackableDB();
    }
    assert(s.ok());
  }

  ~MyTransactionTestBase() {
    if (db) { // delete the db if exists
      delete db;
      db = nullptr;
    }
    // This is to skip the assert statement in FaultInjectionTestEnv. There
    // seems to be a bug in btrfs that the makes readdir return recently
    // unlink-ed files. By using the default fs we simply ignore errors resulted
    // from attempting to delete such files in DestroyDB.
    if (env) {
      options.env = Env::Default();
      delete env;
      env = nullptr;
    }

    if (!keep_db_local_) {
      DestroyDB(dbname, options);
    }
  }

  Status ReOpenNoDelete() {
    delete db;
    db = nullptr;
    env->AssertNoOpenFile();
    env->DropUnsyncedFileData();
    env->ResetState();
    Status s;
    if (use_stackable_db_ == false) {
      s = TransactionDB::Open(options, txn_db_options, dbname, &db);
    } else {
      s = OpenWithStackableDB();
    }
    assert(!s.ok() || db != nullptr);
    return s;
  }

  Status ReOpenNoDelete(std::vector<ColumnFamilyDescriptor>& cfs,
                        std::vector<ColumnFamilyHandle*>* handles) {
    for (auto h : *handles) {
      delete h;
    }
    handles->clear();
    delete db;
    db = nullptr;
    env->AssertNoOpenFile();
    env->DropUnsyncedFileData();
    env->ResetState();
    Status s;
    if (use_stackable_db_ == false) {
      s = TransactionDB::Open(options, txn_db_options, dbname, cfs, handles,
                              &db);
    } else {
      s = OpenWithStackableDB(cfs, handles);
    }
    assert(!s.ok() || db != nullptr);
    return s;
  }

  Status ReOpen() {
    delete db;
    db = nullptr;
    DestroyDB(dbname, options);
    Status s;
    if (use_stackable_db_ == false) {
      s = TransactionDB::Open(options, txn_db_options, dbname, &db);
    } else {
      s = OpenWithStackableDB();
    }
    assert(db != nullptr);
    return s;
  }

  Status OpenWithStackableDB(std::vector<ColumnFamilyDescriptor>& cfs,
                             std::vector<ColumnFamilyHandle*>* handles) {
    std::vector<size_t> compaction_enabled_cf_indices;
    TransactionDB::PrepareWrap(&options, &cfs, &compaction_enabled_cf_indices);
    DB* root_db = nullptr;
    Options options_copy(options);
    const bool use_seq_per_batch =
        txn_db_options.write_policy == WRITE_PREPARED ||
        txn_db_options.write_policy == WRITE_UNPREPARED;
    const bool use_batch_per_txn =
        txn_db_options.write_policy == WRITE_COMMITTED ||
        txn_db_options.write_policy == WRITE_PREPARED;
    Status s = DBImpl::Open(options_copy, dbname, cfs, handles, &root_db,
                            use_seq_per_batch, use_batch_per_txn);
    StackableDB* stackable_db = new StackableDB(root_db);
    if (s.ok()) {
      assert(root_db != nullptr);
      s = TransactionDB::WrapStackableDB(stackable_db, txn_db_options,
                                         compaction_enabled_cf_indices,
                                         *handles, &db);
    }
    if (!s.ok()) {
      delete stackable_db;
    }
    return s;
  }

  Status OpenWithStackableDB() {
    std::vector<size_t> compaction_enabled_cf_indices;
    std::vector<ColumnFamilyDescriptor> column_families{ColumnFamilyDescriptor(
        kDefaultColumnFamilyName, ColumnFamilyOptions(options))};

    TransactionDB::PrepareWrap(&options, &column_families,
                               &compaction_enabled_cf_indices);
    std::vector<ColumnFamilyHandle*> handles;
    DB* root_db = nullptr;
    Options options_copy(options);
    const bool use_seq_per_batch =
        txn_db_options.write_policy == WRITE_PREPARED ||
        txn_db_options.write_policy == WRITE_UNPREPARED;
    const bool use_batch_per_txn =
        txn_db_options.write_policy == WRITE_COMMITTED ||
        txn_db_options.write_policy == WRITE_PREPARED;
    Status s = DBImpl::Open(options_copy, dbname, column_families, &handles,
                            &root_db, use_seq_per_batch, use_batch_per_txn);
    if (!s.ok()) {
      delete root_db;
      return s;
    }
    StackableDB* stackable_db = new StackableDB(root_db);
    assert(root_db != nullptr);
    assert(handles.size() == 1);
    s = TransactionDB::WrapStackableDB(stackable_db, txn_db_options,
                                       compaction_enabled_cf_indices, handles,
                                       &db);
    delete handles[0];
    if (!s.ok()) {
      delete stackable_db;
    }
    return s;
  }

  std::atomic<size_t> linked = {0};
  std::atomic<size_t> exp_seq = {0};
  std::atomic<size_t> commit_writes = {0};
  std::atomic<size_t> expected_commits = {0};
  // Without Prepare, the commit does not write to WAL
  std::atomic<size_t> with_empty_commits = {0};
  std::function<void(size_t, Status)> txn_t0_with_status = [&](size_t index,
                                                               Status exp_s) {
    // Test DB's internal txn. It involves no prepare phase nor a commit marker.
    WriteOptions wopts;
    auto s = db->Put(wopts, "key" + std::to_string(index), "value");
    ASSERT_EQ(exp_s, s);
    if (txn_db_options.write_policy == TxnDBWritePolicy::WRITE_COMMITTED) {
      // Consume one seq per key
      exp_seq++;
    } else {
      // Consume one seq per batch
      exp_seq++;
      if (options.two_write_queues) {
        // Consume one seq for commit
        exp_seq++;
      }
    }
    with_empty_commits++;
  };
  std::function<void(size_t)> txn_t0 = [&](size_t index) {
    return txn_t0_with_status(index, Status::OK());
  };
  std::function<void(size_t)> txn_t1 = [&](size_t index) {
    // Testing directly writing a write batch. Functionality-wise it is
    // equivalent to commit without prepare.
    WriteBatch wb;
    auto istr = std::to_string(index);
     (wb.Put("k1" + istr, "v1"));
     (wb.Put("k2" + istr, "v2"));
     (wb.Put("k3" + istr, "v3"));
    WriteOptions wopts;
    auto s = db->Write(wopts, &wb);
    if (txn_db_options.write_policy == TxnDBWritePolicy::WRITE_COMMITTED) {
      // Consume one seq per key
      exp_seq += 3;
    } else {
      // Consume one seq per batch
      exp_seq++;
      if (options.two_write_queues) {
        // Consume one seq for commit
        exp_seq++;
      }
    }
    // (s);
    with_empty_commits++;
  };
  std::function<void(size_t)> txn_t2 = [&](size_t index) {
    // Commit without prepare. It should write to DB without a commit marker.
    TransactionOptions txn_options;
    WriteOptions write_options;
    Transaction* txn = db->BeginTransaction(write_options, txn_options);
    auto istr = std::to_string(index);
     (txn->SetName("xid" + istr));
     (txn->Put(Slice("foo" + istr), Slice("bar")));
     (txn->Put(Slice("foo2" + istr), Slice("bar2")));
     (txn->Put(Slice("foo3" + istr), Slice("bar3")));
     (txn->Put(Slice("foo4" + istr), Slice("bar4")));
     (txn->Commit());
    if (txn_db_options.write_policy == TxnDBWritePolicy::WRITE_COMMITTED) {
      // Consume one seq per key
      exp_seq += 4;
    } else if (txn_db_options.write_policy ==
               TxnDBWritePolicy::WRITE_PREPARED) {
      // Consume one seq per batch
      exp_seq++;
      if (options.two_write_queues) {
        // Consume one seq for commit
        exp_seq++;
      }
    } else {
      // Flushed after each key, consume one seq per flushed batch
      exp_seq += 4;
      // WriteUnprepared implements CommitWithoutPrepareInternal by simply
      // calling Prepare then Commit. Consume one seq for the prepare.
      exp_seq++;
    }
    delete txn;
    with_empty_commits++;
  };
  std::function<void(size_t)> txn_t3 = [&](size_t index) {
    // A full 2pc txn that also involves a commit marker.
    TransactionOptions txn_options;
    WriteOptions write_options;
    Transaction* txn = db->BeginTransaction(write_options, txn_options);
    auto istr = std::to_string(index);
     (txn->SetName("xid" + istr));
     (txn->Put(Slice("foo" + istr), Slice("bar")));
     (txn->Put(Slice("foo2" + istr), Slice("bar2")));
     (txn->Put(Slice("foo3" + istr), Slice("bar3")));
     (txn->Put(Slice("foo4" + istr), Slice("bar4")));
     (txn->Put(Slice("foo5" + istr), Slice("bar5")));
    expected_commits++;
     (txn->Prepare());
    commit_writes++;
     (txn->Commit());
    if (txn_db_options.write_policy == TxnDBWritePolicy::WRITE_COMMITTED) {
      // Consume one seq per key
      exp_seq += 5;
    } else if (txn_db_options.write_policy ==
               TxnDBWritePolicy::WRITE_PREPARED) {
      // Consume one seq per batch
      exp_seq++;
      // Consume one seq per commit marker
      exp_seq++;
    } else {
      // Flushed after each key, consume one seq per flushed batch
      exp_seq += 5;
      // Consume one seq per commit marker
      exp_seq++;
    }
    delete txn;
  };
  std::function<void(size_t)> txn_t4 = [&](size_t index) {
    // A full 2pc txn that also involves a commit marker.
    TransactionOptions txn_options;
    WriteOptions write_options;
    Transaction* txn = db->BeginTransaction(write_options, txn_options);
    auto istr = std::to_string(index);
     (txn->SetName("xid" + istr));
     (txn->Put(Slice("foo" + istr), Slice("bar")));
     (txn->Put(Slice("foo2" + istr), Slice("bar2")));
     (txn->Put(Slice("foo3" + istr), Slice("bar3")));
     (txn->Put(Slice("foo4" + istr), Slice("bar4")));
     (txn->Put(Slice("foo5" + istr), Slice("bar5")));
    expected_commits++;
     (txn->Prepare());
    commit_writes++;
     (txn->Rollback());
    if (txn_db_options.write_policy == TxnDBWritePolicy::WRITE_COMMITTED) {
      // No seq is consumed for deleting the txn buffer
      exp_seq += 0;
    } else if (txn_db_options.write_policy ==
               TxnDBWritePolicy::WRITE_PREPARED) {
      // Consume one seq per batch
      exp_seq++;
      // Consume one seq per rollback batch
      exp_seq++;
      if (options.two_write_queues) {
        // Consume one seq for rollback commit
        exp_seq++;
      }
    } else {
      // Flushed after each key, consume one seq per flushed batch
      exp_seq += 5;
      // Consume one seq per rollback batch
      exp_seq++;
      if (options.two_write_queues) {
        // Consume one seq for rollback commit
        exp_seq++;
      }
    }
    delete txn;
  };

  // Test that we can change write policy after a clean shutdown (which would
  // empty the WAL)
  void CrossCompatibilityTest(TxnDBWritePolicy from_policy,
                              TxnDBWritePolicy to_policy, bool empty_wal) {
    TransactionOptions txn_options;
    ReadOptions read_options;
    WriteOptions write_options;
    uint32_t index = 0;
    Random rnd(1103);
    options.write_buffer_size = 1024;  // To create more sst files
    std::unordered_map<std::string, std::string> committed_kvs;
    Transaction* txn;

    txn_db_options.write_policy = from_policy;
    if (txn_db_options.write_policy == WRITE_COMMITTED) {
      options.unordered_write = false;
    }
    ReOpen();

    for (int i = 0; i < 1024; i++) {
      auto istr = std::to_string(index);
      auto k = Slice("foo-" + istr).ToString();
      auto v = Slice("bar-" + istr).ToString();
      // For test the duplicate keys
      auto v2 = Slice("bar2-" + istr).ToString();
      auto type = rnd.Uniform(4);
      switch (type) {
        case 0:
          committed_kvs[k] = v;
           (db->Put(write_options, k, v));
          committed_kvs[k] = v2;
           (db->Put(write_options, k, v2));
          break;
        case 1: {
          WriteBatch wb;
          committed_kvs[k] = v;
          wb.Put(k, v);
          committed_kvs[k] = v2;
          wb.Put(k, v2);
           (db->Write(write_options, &wb));

        } break;
        case 2:
        case 3:
          txn = db->BeginTransaction(write_options, txn_options);
           (txn->SetName("xid" + istr));
          committed_kvs[k] = v;
           (txn->Put(k, v));
          committed_kvs[k] = v2;
           (txn->Put(k, v2));

          if (type == 3) {
             (txn->Prepare());
          }
           (txn->Commit());
          delete txn;
          break;
        default:
          assert(0);
      }

      index++;
    }  // for i

    txn_db_options.write_policy = to_policy;
    if (txn_db_options.write_policy == WRITE_COMMITTED) {
      options.unordered_write = false;
    }
    auto db_impl = reinterpret_cast<DBImpl*>(db->GetRootDB());
    // Before upgrade/downgrade the WAL must be emptied
    if (empty_wal) {
      db_impl->TEST_FlushMemTable();
    } else {
      db_impl->FlushWAL(true);
    }
    auto s = ReOpenNoDelete();
    if (empty_wal) {
    //   (s);
    } else {
      // Test that we can detect the WAL that is produced by an incompatible
      // WritePolicy and fail fast before mis-interpreting the WAL.
      ASSERT_TRUE(s.IsNotSupported());
      return;
    }
    db_impl = reinterpret_cast<DBImpl*>(db->GetRootDB());
    // Check that WAL is empty
    VectorLogPtr log_files;
    db_impl->GetSortedWalFiles(log_files);
    ASSERT_EQ(0, log_files.size());

    for (auto& kv : committed_kvs) {
      std::string value;
      s = db->Get(read_options, kv.first, &value);
      if (s.IsNotFound()) {
        printf("key = %s\n", kv.first.c_str());
      }
      // (s);
      if (kv.second != value) {
        printf("key = %s\n", kv.first.c_str());
      }
      ASSERT_EQ(kv.second, value);
    }
  }
};


int main() {
	  MyTransactionTestBase txn_test_base(false, false, WRITE_COMMITTED, kOrderedWrite);
    auto db = txn_test_base.db;
    auto env = txn_test_base.env;
    auto dbname = txn_test_base.dbname;
    auto options = txn_test_base.options;

    ////////////////////////////////////////////////////////////////////////////////////////

    // TransactionDB* db;
    // FaultInjectionTestEnv* env;
    // std::string dbname;
    // Options options;

    // TransactionDBOptions txn_db_options;
    // bool use_stackable_db_;
    // bool keep_db_local_ = true;

    // bool use_stackable_db;
    // bool two_write_queue;
    // TxnDBWritePolicy write_policy;
    // WriteOrdering write_ordering;

    // options.create_if_missing = true;
    // options.max_write_buffer_number = 2;
    // options.write_buffer_size = 4 * 1024;
    // options.unordered_write = write_ordering == kUnorderedWrite;
    // options.level0_file_num_compaction_trigger = 2;
    // options.merge_operator = MergeOperators::CreateFromStringId("stringappend");
    // env = new FaultInjectionTestEnv(Env::Default());
    // options.env = env;
    // options.two_write_queues = two_write_queue;
    // // use perThreadDBPath to avoid conflict with other tests
    // dbname = test::PerThreadDBPath("/home/jiexiao/squint/copy_bug1/alice/workload_dir", "transaction_testdb");

    // DestroyDB(dbname, options);
    // txn_db_options.transaction_lock_timeout = 0;
    // txn_db_options.default_lock_timeout = 0;
    // txn_db_options.write_policy = write_policy;
    // txn_db_options.rollback_merge_operands = true;
    // // This will stress write unprepared, by forcing write batch flush on every
    // // write.
    // txn_db_options.default_write_batch_flush_threshold = 1;
    // // Write unprepared requires all transactions to be named. This setting
    // // autogenerates the name so that existing tests can pass.
    // // txn_db_options.autogenerate_name = true;
    // Status s;
    // if (use_stackable_db == false) {
    //   s = TransactionDB::Open(options, txn_db_options, dbname, &db);
    // } else {
    //   s = OpenWithStackableDB();
    // }
    // assert(s.ok());

    ////////////////////////// CREATE TABLE //////////////////////////
    /////////////////////// DOUBLE CRASH TEST ////////////////////////
    for (const bool manual_wal_flush : {false, true}) {
      for (const bool write_after_recovery : {false, true}) {
        options.wal_recovery_mode = WALRecoveryMode::kPointInTimeRecovery;
        options.manual_wal_flush = manual_wal_flush;
        txn_test_base.ReOpen();
        std::string cf_name = "two";
        ColumnFamilyOptions cf_options;
        ColumnFamilyHandle* cf_handle = nullptr;
        (db->CreateColumnFamily(cf_options, cf_name, &cf_handle));

        // Add a prepare entry to prevent the older logs from being deleted.
        WriteOptions write_options;
        TransactionOptions txn_options;
        Transaction* txn = db->BeginTransaction(write_options, txn_options);
        (txn->SetName("xid"));
        (txn->Put(Slice("foo-prepare"), Slice("bar-prepare")));
        (txn->Prepare());

        FlushOptions flush_ops;
        db->Flush(flush_ops);
        // Now we have a log that cannot be deleted

        (db->Put(write_options, cf_handle, "foo1", "bar1"));
        // Flush only the 2nd cf
        db->Flush(flush_ops, cf_handle);

        // The value is large enough to be touched by the corruption we ingest
        // below.
        std::string large_value(400, ' ');
        // key/value not touched by corruption
        (db->Put(write_options, "foo2", "bar2"));
        // key/value touched by corruption
        (db->Put(write_options, "foo3", large_value));
        // key/value not touched by corruption
        (db->Put(write_options, "foo4", "bar4"));

        db->FlushWAL(true);
        DBImpl* db_impl = reinterpret_cast<DBImpl*>(db->GetRootDB());
        uint64_t wal_file_id = db_impl->TEST_LogfileNumber();
        std::string fname = LogFileName(dbname, wal_file_id);
        // reinterpret_cast<PessimisticTransactionDB*>(db)->TEST_Crash();
        delete txn;
        delete cf_handle;
        delete db;
        db = nullptr;

        // Corrupt the last log file in the middle, so that it is not corrupted
        // in the tail.
        std::string file_content;
        (ReadFileToString(env, fname, &file_content));
        file_content[400] = 'h';
        file_content[401] = 'a';
        (env->DeleteFile(fname));
        (WriteStringToFile(env, file_content, fname, true));

        // Recover from corruption
        std::vector<ColumnFamilyHandle*> handles;
        std::vector<ColumnFamilyDescriptor> column_families;
        column_families.push_back(ColumnFamilyDescriptor(kDefaultColumnFamilyName,
                                                        ColumnFamilyOptions()));
        column_families.push_back(
            ColumnFamilyDescriptor("two", ColumnFamilyOptions()));
        (txn_test_base.ReOpenNoDelete(column_families, &handles));

        if (write_after_recovery) {
          // Write data to the log right after the corrupted log
          (db->Put(write_options, "foo5", large_value));
        }

        // Persist data written to WAL during recovery or by the last Put
        db->FlushWAL(true);
        // 2nd crash to recover while having a valid log after the corrupted one.
        (txn_test_base.ReOpenNoDelete(column_families, &handles));
        assert(db != nullptr);
        txn = db->GetTransactionByName("xid");
        assert(txn != nullptr);
        (txn->Commit());
        delete txn;
        for (auto handle : handles) {
          delete handle;
        }
      }
  }
}