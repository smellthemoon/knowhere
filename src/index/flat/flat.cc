// Copyright (C) 2019-2023 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

#include "common/metric.h"
#include "common/range_util.h"
#include "faiss/IndexBinaryFlat.h"
#include "faiss/IndexFlat.h"
#include "faiss/index_io.h"
#include "index/flat/flat_config.h"
#include "io/FaissIO.h"
#include "knowhere/comp/thread_pool.h"
#include "knowhere/factory.h"

namespace knowhere {

template <typename T>
class FlatIndexNode : public IndexNode {
 public:
    FlatIndexNode(const Object&) : index_(nullptr) {
        static_assert(std::is_same<T, faiss::IndexFlat>::value || std::is_same<T, faiss::IndexBinaryFlat>::value,
                      "not suppprt.");
        pool_ = ThreadPool::GetGlobalThreadPool();
    }

    Status
    Build(const DataSet& dataset, const Config& cfg) override {
        auto err = Train(dataset, cfg);
        if (err != Status::success) {
            return err;
        }
        err = Add(dataset, cfg);
        return err;
    }

    Status
    Train(const DataSet&, const Config&) override {
        return Status::success;
    }

    Status
    Add(const DataSet& dataset, const Config& cfg) override {
        T* index = nullptr;
        const FlatConfig& f_cfg = static_cast<const FlatConfig&>(cfg);
        auto metric = Str2FaissMetricType(f_cfg.metric_type);
        if (!metric.has_value()) {
            LOG_KNOWHERE_WARNING_ << "please check metric type, " << f_cfg.metric_type;
            return metric.error();
        }
        index = new (std::nothrow) T(dataset.GetDim(), metric.value());

        if (index == nullptr) {
            LOG_KNOWHERE_WARNING_ << "memory malloc error";
            return Status::malloc_error;
        }

        if (this->index_) {
            delete this->index_;
            LOG_KNOWHERE_WARNING_ << "index not empty, deleted old index";
        }
        this->index_ = index;
        const void* x = dataset.GetTensor();
        const int64_t n = dataset.GetRows();
        if constexpr (std::is_same<T, faiss::IndexFlat>::value) {
            index_->add(n, (const float*)x);
        }
        if constexpr (std::is_same<T, faiss::IndexBinaryFlat>::value) {
            index_->add(n, (const uint8_t*)x);
        }
        return Status::success;
    }

    expected<DataSetPtr, Status>
    Search(const DataSet& dataset, const Config& cfg, const BitsetView& bitset) const override {
        if (!index_) {
            LOG_KNOWHERE_WARNING_ << "search on empty index";
            return unexpected(Status::empty_index);
        }

        DataSetPtr results = std::make_shared<DataSet>();
        const FlatConfig& f_cfg = static_cast<const FlatConfig&>(cfg);
        auto nq = dataset.GetRows();
        auto x = dataset.GetTensor();
        auto len = f_cfg.k * nq;
        int64_t* ids = nullptr;
        float* dis = nullptr;
        try {
            ids = new (std::nothrow) int64_t[len];
            dis = new (std::nothrow) float[len];
            std::vector<std::future<void>> futs;
            futs.reserve(nq);
            for (int i = 0; i < nq; ++i) {
                futs.push_back(pool_->push([&, index = i] {
                    ThreadPool::ScopedOmpSetter setter(1);
                    auto cur_ids = ids + f_cfg.k * index;
                    auto cur_dis = dis + f_cfg.k * index;
                    if constexpr (std::is_same<T, faiss::IndexFlat>::value) {
                        index_->search(1, (const float*)x + index * Dim(), f_cfg.k, cur_dis, cur_ids, bitset);
                    }
                    if constexpr (std::is_same<T, faiss::IndexBinaryFlat>::value) {
                        auto cur_i_dis = reinterpret_cast<int32_t*>(cur_dis);
                        index_->search(1, (const uint8_t*)x + index * Dim(), f_cfg.k, cur_i_dis, cur_ids, bitset);
                        if (index_->metric_type == faiss::METRIC_Hamming) {
                            for (int64_t j = 0; j < f_cfg.k; j++) {
                                cur_dis[j] = static_cast<float>(cur_i_dis[j]);
                            }
                        }
                    }
                }));
            }
            for (auto& fut : futs) {
                fut.get();
            }
        } catch (const std::exception& e) {
            std::unique_ptr<int64_t[]> auto_delete_ids(ids);
            std::unique_ptr<float[]> auto_delete_dis(dis);
            LOG_KNOWHERE_WARNING_ << "error inner faiss, " << e.what();
            return unexpected(Status::faiss_inner_error);
        }

        return GenResultDataSet(nq, f_cfg.k, ids, dis);
    }

    expected<DataSetPtr, Status>
    RangeSearch(const DataSet& dataset, const Config& cfg, const BitsetView& bitset) const override {
        if (!index_) {
            LOG_KNOWHERE_WARNING_ << "range search on empty index.";
            return unexpected(Status::empty_index);
        }

        const FlatConfig& f_cfg = static_cast<const FlatConfig&>(cfg);
        auto nq = dataset.GetRows();
        auto xq = dataset.GetTensor();

        int64_t* ids = nullptr;
        float* distances = nullptr;
        size_t* lims = nullptr;
        try {
            float radius = f_cfg.radius;
            bool is_ip = index_->metric_type == faiss::METRIC_INNER_PRODUCT && std::is_same_v<T, faiss::IndexFlat>;
            float range_filter = f_cfg.range_filter;
            std::vector<std::vector<int64_t>> result_id_array(nq);
            std::vector<std::vector<float>> result_dist_array(nq);
            std::vector<size_t> result_size(nq);
            std::vector<size_t> result_lims(nq + 1);
            std::vector<std::future<void>> futs;
            futs.reserve(nq);
            for (int i = 0; i < nq; ++i) {
                futs.push_back(pool_->push([&, index = i] {
                    ThreadPool::ScopedOmpSetter setter(1);
                    faiss::RangeSearchResult res(1);
                    if constexpr (std::is_same<T, faiss::IndexFlat>::value) {
                        index_->range_search(1, (const float*)xq + index * Dim(), radius, &res, bitset);
                    }
                    if constexpr (std::is_same<T, faiss::IndexBinaryFlat>::value) {
                        index_->range_search(1, (const uint8_t*)xq + index * Dim(), radius, &res, bitset);
                    }
                    auto elem_cnt = res.lims[1];
                    result_dist_array[index].resize(elem_cnt);
                    result_id_array[index].resize(elem_cnt);
                    result_size[index] = elem_cnt;
                    for (size_t j = 0; j < elem_cnt; j++) {
                        result_dist_array[index][j] = res.distances[j];
                        result_id_array[index][j] = res.labels[j];
                    }
                    if (f_cfg.range_filter != defaultRangeFilter) {
                        FilterRangeSearchResultForOneNq(result_dist_array[index], result_id_array[index], is_ip, radius,
                                                        range_filter);
                    }
                }));
            }
            for (auto& fut : futs) {
                fut.get();
            }
            GetRangeSearchResult(result_dist_array, result_id_array, is_ip, nq, radius, range_filter, distances, ids,
                                 lims);
        } catch (const std::exception& e) {
            LOG_KNOWHERE_WARNING_ << "error inner faiss, " << e.what();
            return unexpected(Status::faiss_inner_error);
        }

        return GenResultDataSet(nq, ids, distances, lims);
    }

    expected<DataSetPtr, Status>
    GetVectorByIds(const DataSet& dataset, const Config& cfg) const override {
        auto nq = dataset.GetRows();
        auto dim = dataset.GetDim();
        auto in_ids = dataset.GetIds();
        if constexpr (std::is_same<T, faiss::IndexFlat>::value) {
            try {
                float* xq = new (std::nothrow) float[nq * dim];
                for (int64_t i = 0; i < nq; i++) {
                    int64_t id = in_ids[i];
                    index_->reconstruct(id, xq + i * dim);
                }
                return GenResultDataSet(xq);
            } catch (const std::exception& e) {
                LOG_KNOWHERE_WARNING_ << "faiss inner error: " << e.what();
                return unexpected(Status::faiss_inner_error);
            }
        }
        if constexpr (std::is_same<T, faiss::IndexBinaryFlat>::value) {
            try {
                uint8_t* xq = new (std::nothrow) uint8_t[nq * dim / 8];
                for (int64_t i = 0; i < nq; i++) {
                    int64_t id = in_ids[i];
                    index_->reconstruct(id, xq + i * dim / 8);
                }
                return GenResultDataSet(xq);
            } catch (const std::exception& e) {
                LOG_KNOWHERE_WARNING_ << "error inner faiss, " << e.what();
                return unexpected(Status::faiss_inner_error);
            }
        }
    }

    expected<DataSetPtr, Status>
    GetIndexMeta(const Config& cfg) const override {
        return unexpected(Status::not_implemented);
    }

    Status
    Serialize(BinarySet& binset) const override {
        if (!index_) {
            return Status::empty_index;
        }
        try {
            MemoryIOWriter writer;
            if constexpr (std::is_same<T, faiss::IndexFlat>::value) {
                faiss::write_index(index_, &writer);
            }
            if constexpr (std::is_same<T, faiss::IndexBinaryFlat>::value) {
                faiss::write_index_binary(index_, &writer);
            }
            std::shared_ptr<uint8_t[]> data(writer.data_);
            if constexpr (std::is_same<T, faiss::IndexFlat>::value) {
                binset.Append("FLAT", data, writer.rp);
            }
            if constexpr (std::is_same<T, faiss::IndexBinaryFlat>::value) {
                binset.Append("BinaryIVF", data, writer.rp);
            }
            return Status::success;
        } catch (const std::exception& e) {
            LOG_KNOWHERE_WARNING_ << "error inner faiss, " << e.what();
            return Status::faiss_inner_error;
        }
    }

    Status
    Deserialize(const BinarySet& binset) override {
        if (index_) {
            delete index_;
            index_ = nullptr;
        }
        std::string name = "";
        if constexpr (std::is_same<T, faiss::IndexFlat>::value) {
            name = "FLAT";
        }
        if constexpr (std::is_same<T, faiss::IndexBinaryFlat>::value) {
            name = "BinaryIVF";
        }
        auto binary = binset.GetByName(name);

        MemoryIOReader reader;
        reader.total = binary->size;
        reader.data_ = binary->data.get();
        if constexpr (std::is_same<T, faiss::IndexFlat>::value) {
            faiss::Index* index = faiss::read_index(&reader);
            index_ = static_cast<T*>(index);
        }
        if constexpr (std::is_same<T, faiss::IndexBinaryFlat>::value) {
            faiss::IndexBinary* index = faiss::read_index_binary(&reader);
            index_ = static_cast<T*>(index);
        }
        return Status::success;
    }

    std::unique_ptr<BaseConfig>
    CreateConfig() const override {
        return std::make_unique<FlatConfig>();
    }

    int64_t
    Dim() const override {
        return index_->d;
    }

    int64_t
    Size() const override {
        return index_->ntotal * index_->d * sizeof(float);
    }

    int64_t
    Count() const override {
        return index_->ntotal;
    }

    std::string
    Type() const override {
        if constexpr (std::is_same<T, faiss::IndexFlat>::value) {
            return knowhere::IndexEnum::INDEX_FAISS_IDMAP;
        }
        if constexpr (std::is_same<T, faiss::IndexBinaryFlat>::value) {
            return knowhere::IndexEnum::INDEX_FAISS_BIN_IVFFLAT;
        }
    }

    ~FlatIndexNode() override {
        if (index_) {
            delete index_;
        }
    }

 private:
    T* index_;
    std::shared_ptr<ThreadPool> pool_;
};

KNOWHERE_REGISTER_GLOBAL(FLAT,
                         [](const Object& object) { return Index<FlatIndexNode<faiss::IndexFlat>>::Create(object); });
KNOWHERE_REGISTER_GLOBAL(BINFLAT, [](const Object& object) {
    return Index<FlatIndexNode<faiss::IndexBinaryFlat>>::Create(object);
});
KNOWHERE_REGISTER_GLOBAL(BIN_FLAT, [](const Object& object) {
    return Index<FlatIndexNode<faiss::IndexBinaryFlat>>::Create(object);
});

}  // namespace knowhere
   //
