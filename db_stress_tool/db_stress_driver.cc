//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//

#ifdef GFLAGS
#include "db_stress_tool/db_stress_common.h"
#include "utilities/fault_injection_fs.h"

namespace ROCKSDB_NAMESPACE {
void ThreadBody(void* v) {
  ThreadState* thread = reinterpret_cast<ThreadState*>(v);
  SharedState* shared = thread->shared;

  if (!FLAGS_skip_verifydb && shared->ShouldVerifyAtBeginning()) {
    thread->shared->GetStressTest()->VerifyDb(thread);
  }
  {
    MutexLock l(shared->GetMutex());
    shared->IncInitialized();
    if (shared->AllInitialized()) {
      shared->GetCondVar()->SignalAll();
    }
    while (!shared->Started()) {
      shared->GetCondVar()->Wait();
    }
  }

  if (FLAGS_squint_mode != "init" && FLAGS_squint_mode != "checker") {
    thread->shared->GetStressTest()->OperateDb(thread);
  }
  

  {
    MutexLock l(shared->GetMutex());
    shared->IncOperated();
    if (shared->AllOperated()) {
      fprintf(stdout, "All threads done operating\n");
      shared->GetCondVar()->SignalAll();
    }
    fprintf(stdout, "Thread %d done\n", thread->tid);

    if (FLAGS_squint_mode == "workload") return;
    while (!shared->VerifyStarted()) {
      shared->GetCondVar()->Wait();
    }
  }


  if (!FLAGS_skip_verifydb) {
    thread->shared->GetStressTest()->VerifyDb(thread);
  }

  {
    MutexLock l(shared->GetMutex());
    shared->IncDone();
    if (shared->AllDone()) {
      shared->GetCondVar()->SignalAll();
    }
  }
}

int VerifySetCurrentFile(std::string directoryPath) {
std::string logFilePath = directoryPath + "/LOG";
    std::string currentFilePath = directoryPath + "/CURRENT";

    // Check if both files exist
    if (!std::filesystem::exists(logFilePath) || !std::filesystem::exists(currentFilePath)) {
        std::cerr << "Error: Files LOG or CURRENT not found in the specified directory." << std::endl;
        return 1;
    }

    std::ifstream logFile(logFilePath);
    std::string line;
    std::string lastSetCurrentFile;

    // Read the LOG file and find the last occurrence of "SetCurrentFile: xxx"
    while (getline(logFile, line)) {
        std::size_t found = line.find("SetCurrentFile:");
        if (found != std::string::npos) {
            lastSetCurrentFile = line.substr(found + 15); // 15 is the length of "SetCurrentFile:"
        }
    }
    logFile.close();

    // Trim potential whitespace
    lastSetCurrentFile.erase(0, lastSetCurrentFile.find_first_not_of(" \n\r\t"));
    lastSetCurrentFile.erase(lastSetCurrentFile.find_last_not_of(" \n\r\t") + 1);

    // Also trim any "/" at first
    if (lastSetCurrentFile.front() == '/') {
        lastSetCurrentFile.erase(0, 1);
    }

    if (lastSetCurrentFile.empty()) {
        std::cerr << "Error: No 'SetCurrentFile' entry found in LOG." << std::endl;
        return 1;
    }

    // get the MANIFEST number
    std::size_t found = lastSetCurrentFile.find("MANIFEST-");
    unsigned long long logManifestNumber = std::stoull(lastSetCurrentFile.substr(found + 9)); // 9 is the length of "MANIFEST-"

    // Read the CURRENT file and compare its contents with lastSetCurrentFile
    std::ifstream currentFile(currentFilePath);
    std::string currentContent;
    getline(currentFile, currentContent);
    currentFile.close();

    // Trim potential whitespace
    currentContent.erase(0, currentContent.find_first_not_of(" \n\r\t"));
    currentContent.erase(currentContent.find_last_not_of(" \n\r\t") + 1);

    // Get the MANIFEST number from the CURRENT file
    found = currentContent.find("MANIFEST-");
    unsigned long long currentManifestNumber = std::stoull(currentContent.substr(found + 9)); // 9 is the length of "MANIFEST-"

    // Compare and return results based on comparison
    // the number in the LOG should be smaller than or equal to the number in the CURRENT
    if (logManifestNumber > currentManifestNumber) {
        std::cerr << "Error: MANIFEST number in LOG is greater than the one in CURRENT." << std::endl;
        return 1;
    } else {
        std::cout << "MANIFEST number in LOG and CURRENT match." << std::endl;
        return 0;
    }
}

bool RunStressTest(StressTest* stress) {
  SystemClock* clock = db_stress_env->GetSystemClock().get();
  SharedState shared(db_stress_env, stress);
  stress->InitDb(&shared);
  stress->FinishInitDb(&shared);

  if (FLAGS_squint_mode == "init") {
    fprintf(stdout, "db_stress <> squint: Initialization done\n");
    return true;
  }

  if (FLAGS_squint_mode == "checker") {
    if (FLAGS_simple_verify) {
      fprintf(stdout, "db_stress <> squint: Checker simply verify started\n");
      stress->PrintKVCount();
      fprintf(stdout, "db_stress <> squint: Checker starting SetCurrentFile verification process\n");
      if (VerifySetCurrentFile(FLAGS_db)) {
        fprintf(stdout, "db_stress <> squint: Checker SetCurrentFile verification failed\n");
        return false;
      }
      fprintf(stdout, "db_stress <> squint: Checker simply verify done\n");
      return true;
    }
    else {
      if (FLAGS_opfile_path == "") {
        fprintf(stderr, "db_stress <> squint: Checker opfile path not provided, cannot verify! Please provide opfile path or use simple verify \n");
        return false;
      }
      fprintf(stdout, "db_stress <> squint: Checker opfile verify started, reading opfile \n");
      shared.ReadOpFile(FLAGS_opfile_path, FLAGS_ops_completed_path);

    }
  }

  if (FLAGS_sync_fault_injection) {
    fault_fs_guard->SetFilesystemDirectWritable(false);
  }
  if (FLAGS_write_fault_one_in) {
    fault_fs_guard->EnableWriteErrorInjection();
  }

  uint32_t n = FLAGS_threads;
  uint64_t now = clock->NowMicros();
  fprintf(stdout, "%s Initializing worker threads\n",
          clock->TimeToString(now / 1000000).c_str());

  shared.SetThreads(n);

  if (FLAGS_compaction_thread_pool_adjust_interval > 0) {
    shared.IncBgThreads();
  }

  if (FLAGS_continuous_verification_interval > 0) {
    shared.IncBgThreads();
  }

  std::vector<ThreadState*> threads(n);
  for (uint32_t i = 0; i < n; i++) {
    threads[i] = new ThreadState(i, &shared);
    db_stress_env->StartThread(ThreadBody, threads[i]);
  }

  ThreadState bg_thread(0, &shared);
  if (FLAGS_compaction_thread_pool_adjust_interval > 0) {
    db_stress_env->StartThread(PoolSizeChangeThread, &bg_thread);
  }

  ThreadState continuous_verification_thread(0, &shared);
  if (FLAGS_continuous_verification_interval > 0) {
    db_stress_env->StartThread(DbVerificationThread,
                               &continuous_verification_thread);
  }

  // Each thread goes through the following states:
  // initializing -> wait for others to init -> read/populate/depopulate
  // wait for others to operate -> verify -> done

  {
    MutexLock l(shared.GetMutex());
    while (!shared.AllInitialized()) {
      shared.GetCondVar()->Wait();
    }
    if (shared.ShouldVerifyAtBeginning()) {
      if (shared.HasVerificationFailedYet()) {
        fprintf(stderr, "Crash-recovery verification failed :(\n");
      } else {
        fprintf(stdout, "Crash-recovery verification passed :)\n");
      }
    }

    // This is after the verification step to avoid making all those `Get()`s
    // and `MultiGet()`s contend on the DB-wide trace mutex.
    stress->TrackExpectedState(&shared);

    now = clock->NowMicros();
    fprintf(stdout, "%s Starting database operations\n",
            clock->TimeToString(now / 1000000).c_str());

    shared.SetStart();
    shared.GetCondVar()->SignalAll();
    while (!shared.AllOperated()) {
      fprintf(stdout, "Waiting for all threads to operate\n");
      shared.GetCondVar()->Wait();
    }

    // print out the number of keys in the database
    stress->PrintKVCount();


    if (FLAGS_squint_mode == "workload") {
      fprintf(stdout, "db_stress <> squint: Workload done\n");
      return true;
    }

    now = clock->NowMicros();
    if (FLAGS_test_batches_snapshots) {
      fprintf(stdout, "%s Limited verification already done during gets\n",
              clock->TimeToString((uint64_t)now / 1000000).c_str());
    } else if (FLAGS_skip_verifydb) {
      fprintf(stdout, "%s Verification skipped\n",
              clock->TimeToString((uint64_t)now / 1000000).c_str());
    } else {
      fprintf(stdout, "%s Starting verification\n",
              clock->TimeToString((uint64_t)now / 1000000).c_str());
    }

    shared.SetStartVerify();
    shared.GetCondVar()->SignalAll();
    while (!shared.AllDone()) {
      shared.GetCondVar()->Wait();
    }
  }

  bool checker_failed = false;
  if (FLAGS_squint_mode == "checker") {
    fprintf(stdout, "db_stress <> squint: Checker original verification process completes\n");
    fprintf(stdout, "db_stress <> squint: Checker starting SetCurrentFile verification process\n");
    if (VerifySetCurrentFile(FLAGS_db)) {
      fprintf(stdout, "db_stress <> squint: Checker SetCurrentFile verification failed\n");
      checker_failed = true;
    } else {
      fprintf(stdout, "db_stress <> squint: Checker SetCurrentFile verification passed\n");
    }
    fprintf(stdout, "db_stress <> squint: Checker done\n");
  }

  for (unsigned int i = 1; i < n; i++) {
    threads[0]->stats.Merge(threads[i]->stats);
  }
  threads[0]->stats.Report("Stress Test");

  for (unsigned int i = 0; i < n; i++) {
    delete threads[i];
    threads[i] = nullptr;
  }
  now = clock->NowMicros();
  if (!FLAGS_skip_verifydb && !FLAGS_test_batches_snapshots &&
      !shared.HasVerificationFailedYet()) {
    fprintf(stdout, "%s Verification successful\n",
            clock->TimeToString(now / 1000000).c_str());
  }
  stress->PrintStatistics();

  if (FLAGS_compaction_thread_pool_adjust_interval > 0 ||
      FLAGS_continuous_verification_interval > 0) {
    MutexLock l(shared.GetMutex());
    shared.SetShouldStopBgThread();
    while (!shared.BgThreadsFinished()) {
      shared.GetCondVar()->Wait();
    }
  }

  if (shared.HasVerificationFailedYet() || checker_failed) {
    fprintf(stderr, "Verification failed :(\n");
    return false;
  }
  return true;
}
}  // namespace ROCKSDB_NAMESPACE
#endif  // GFLAGS
