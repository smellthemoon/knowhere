// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#include "common/Utils.h"

#include <stdio.h>
#include <unistd.h>

#include "common/Log.h"

namespace knowhere::utils {

#define MAX_THREAD_NUM 100000
#define MIN_THREAD_NUM 0
#define THREAD_LENGTH 7  // the length of max_thread_num +1

int32_t
GetBuildOmpThread(const Config& conf) {
    return CheckKeyInConfig(conf, meta::BUILD_INDEX_OMP_NUM) ? GetMetaBuildIndexOmpNum(conf) : omp_get_max_threads();
}

int32_t
GetQueryOmpThread(const Config& conf) {
    return CheckKeyInConfig(conf, meta::QUERY_OMP_NUM) ? GetMetaQueryOmpNum(conf) : omp_get_max_threads();
}

void
SetBuildOmpThread(const Config& conf) {
    int32_t omp_num = GetBuildOmpThread(conf);
    omp_set_num_threads(omp_num);
    LOG_KNOWHERE_DEBUG_ << "Set current omp thread num for build: " << omp_num;
}

void
SetQueryOmpThread(const Config& conf) {
    int32_t omp_num = GetQueryOmpThread(conf);
    omp_set_num_threads(omp_num);
    LOG_KNOWHERE_DEBUG_ << "Set current omp thread num for query: " << omp_num;
}

int32_t
GetThreadNum(int id) {
    std::string cmd = "ps -p " + std::to_string(id) + " -Tf | wc -l";
    FILE* file = popen(cmd.c_str(), "r");
    char fBuff[THREAD_LENGTH];
    if (fgets(fBuff, sizeof(fBuff), file) == nullptr) {
        KNOWHERE_THROW_MSG("could not open the file to get thread number");
    }
    std::cout << fBuff << std::endl;
    pclose(file);
    const std::size_t len = strlen(fBuff);
    if (len > 0 && fBuff[len - 1] == '\n') {
        fBuff[len - 1] = '\0';
    }
    int32_t ans = atoi(fBuff);
    std::cout << ans << std::endl;
    if (ans < MIN_THREAD_NUM || ans > MAX_THREAD_NUM) {
        KNOWHERE_THROW_MSG("thread number is out of control");
    }
    return ans;
}

}  // namespace knowhere::utils
