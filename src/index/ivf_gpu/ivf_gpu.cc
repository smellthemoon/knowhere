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
#include "faiss/IndexFlat.h"
#include "faiss/IndexIVFFlat.h"
#include "faiss/IndexIVFPQ.h"
#include "faiss/IndexReplicas.h"
#include "faiss/IndexScalarQuantizer.h"
#include "faiss/gpu/GpuCloner.h"
#include "faiss/gpu/GpuIndexIVF.h"
#include "faiss/gpu/GpuIndexIVFFlat.h"
#include "faiss/gpu/GpuIndexIVFPQ.h"
#include "faiss/gpu/GpuIndexIVFScalarQuantizer.h"
#include "faiss/index_io.h"
#include "index/ivf_gpu/ivf_gpu_config.h"
#include "io/FaissIO.h"
#include "knowhere/comp/index_param.h"
#include "knowhere/factory.h"
#include "knowhere/gpu/gpu_res_mgr.h"

namespace knowhere {

template <typename T>
struct KnowhereConfigType {};

template <>
struct KnowhereConfigType<faiss::IndexIVFFlat> {
    typedef GpuIvfFlatConfig Type;
};
template <>
struct KnowhereConfigType<faiss::IndexIVFPQ> {
    typedef GpuIvfPqConfig Type;
};
template <>
struct KnowhereConfigType<faiss::IndexIVFScalarQuantizer> {
    typedef GpuIvfSqConfig Type;
};

template <typename T>
class GpuIvfIndexNode : public IndexNode {
 public:
    GpuIvfIndexNode(const Object& object) : gpu_index_(nullptr) {
        static_assert(std::is_same<T, faiss::IndexIVFFlat>::value || std::is_same<T, faiss::IndexIVFPQ>::value ||
                      std::is_same<T, faiss::IndexIVFScalarQuantizer>::value);
    }

    virtual Status
    Build(const DataSet& dataset, const Config& cfg) override {
        auto err = Train(dataset, cfg);
        if (err != Status::success)
            return err;
        return Add(dataset, cfg);
    }

    virtual Status
    Train(const DataSet& dataset, const Config& cfg) override {
        if (gpu_index_ && gpu_index_->is_trained) {
            LOG_KNOWHERE_WARNING_ << "index is already trained";
            return Status::index_already_trained;
        }

        auto rows = dataset.GetRows();
        auto tensor = dataset.GetTensor();
        auto dim = dataset.GetDim();
        auto ivf_gpu_cfg = static_cast<const typename KnowhereConfigType<T>::Type&>(cfg);

        auto metric = Str2FaissMetricType(ivf_gpu_cfg.metric_type);
        if (!metric.has_value()) {
            LOG_KNOWHERE_WARNING_ << "please check metric value: " << ivf_gpu_cfg.metric_type;
            return metric.error();
        }

        faiss::Index* gpu_index = nullptr;
        try {
            auto gpu_res = GPUResMgr::GetInstance().GetRes();
            ResScope rs(gpu_res, true);

            if constexpr (std::is_same<T, faiss::IndexIVFFlat>::value) {
                faiss::gpu::GpuIndexIVFFlatConfig f_cfg;
                f_cfg.device = static_cast<int32_t>(gpu_res->gpu_id_);
                gpu_index = new faiss::gpu::GpuIndexIVFFlat(gpu_res->faiss_res_.get(), dim, ivf_gpu_cfg.nlist,
                                                            metric.value(), f_cfg);
            }
            if constexpr (std::is_same<T, faiss::IndexIVFPQ>::value) {
                faiss::gpu::GpuIndexIVFPQConfig f_cfg;
                f_cfg.device = static_cast<int32_t>(gpu_res->gpu_id_);
                gpu_index = new faiss::gpu::GpuIndexIVFPQ(gpu_res->faiss_res_.get(), dim, ivf_gpu_cfg.nlist,
                                                          ivf_gpu_cfg.m, ivf_gpu_cfg.nbits, metric.value(), f_cfg);
            }
            if constexpr (std::is_same<T, faiss::IndexIVFScalarQuantizer>::value) {
                faiss::gpu::GpuIndexIVFScalarQuantizerConfig f_cfg;
                f_cfg.device = static_cast<int32_t>(gpu_res->gpu_id_);
                gpu_index = new faiss::gpu::GpuIndexIVFScalarQuantizer(gpu_res->faiss_res_.get(), dim,
                                                                       ivf_gpu_cfg.nlist, faiss::QuantizerType::QT_8bit,
                                                                       metric.value(), true, f_cfg);
            }

            gpu_index->train(rows, reinterpret_cast<const float*>(tensor));
            res_ = gpu_res;
        } catch (std::exception& e) {
            if (gpu_index) {
                delete gpu_index;
            }
            LOG_KNOWHERE_WARNING_ << "faiss inner error, " << e.what();
            return Status::faiss_inner_error;
        }
        this->gpu_index_ = gpu_index;
        return Status::success;
    }

    virtual Status
    Add(const DataSet& dataset, const Config& cfg) override {
        if (!gpu_index_)
            return Status::empty_index;
        if (!gpu_index_->is_trained)
            return Status::index_not_trained;
        auto rows = dataset.GetRows();
        auto tensor = dataset.GetTensor();
        try {
            ResScope rs(res_, false);
            gpu_index_->add(rows, (const float*)tensor);
        } catch (std::exception& e) {
            LOG_KNOWHERE_WARNING_ << "faiss inner error, " << e.what();
            return Status::faiss_inner_error;
        }
        return Status::success;
    }

    virtual expected<DataSetPtr, Status>
    Search(const DataSet& dataset, const Config& cfg, const BitsetView& bitset) const override {
        auto ivf_gpu_cfg = static_cast<const typename KnowhereConfigType<T>::Type&>(cfg);
        if (auto ix = dynamic_cast<faiss::gpu::GpuIndexIVF*>(gpu_index_)) {
            ix->setNumProbes(ivf_gpu_cfg.nprobe);
        }
        ResScope rs(res_, false);

        constexpr int64_t block_size = 2048;
        auto rows = dataset.GetRows();
        auto k = ivf_gpu_cfg.k;
        auto tensor = dataset.GetTensor();
        auto dim = dataset.GetDim();
        float* dis = new (std::nothrow) float[rows * k];
        int64_t* ids = new (std::nothrow) int64_t[rows * k];
        try {
            for (int i = 0; i < rows; i += block_size) {
                int64_t search_size = (rows - i > block_size) ? block_size : (rows - i);
                gpu_index_->search(search_size, reinterpret_cast<const float*>(tensor) + i * dim, k, dis + i * k,
                                   ids + i * k, bitset);
            }
        } catch (std::exception& e) {
            std::unique_ptr<float> auto_delete_dis(dis);
            std::unique_ptr<int64_t> auto_delete_ids(ids);
            LOG_KNOWHERE_WARNING_ << "faiss inner error, " << e.what();
            return unexpected(Status::faiss_inner_error);
        }

        return GenResultDataSet(rows, ivf_gpu_cfg.k, ids, dis);
    }

    expected<DataSetPtr, Status>
    RangeSearch(const DataSet& dataset, const Config& cfg, const BitsetView& bitset) const override {
        return unexpected(Status::not_implemented);
    }

    virtual expected<DataSetPtr, Status>
    GetVectorByIds(const DataSet& dataset, const Config& cfg) const override {
        return unexpected(Status::not_implemented);
    }

    expected<DataSetPtr, Status>
    GetIndexMeta(const Config& cfg) const override {
        return unexpected(Status::not_implemented);
    }

    virtual Status
    Serialize(BinarySet& binset) const override {
        if (!this->gpu_index_)
            return Status::empty_index;
        if (!this->gpu_index_->is_trained)
            return Status::index_not_trained;

        try {
            MemoryIOWriter writer;
            {
                faiss::Index* host_index = faiss::gpu::index_gpu_to_cpu(gpu_index_);
                faiss::write_index(host_index, &writer);
                delete host_index;
            }
            std::shared_ptr<uint8_t[]> data(writer.data_);
            binset.Append("IVF", data, writer.rp);
        } catch (std::exception& e) {
            LOG_KNOWHERE_WARNING_ << "faiss inner error, " << e.what();
            return Status::faiss_inner_error;
        }

        return Status::success;
    }

    virtual Status
    Deserialize(const BinarySet& binset) override {
        auto binary = binset.GetByName("IVF");
        MemoryIOReader reader;
        try {
            reader.total = binary->size;
            reader.data_ = binary->data.get();

            std::unique_ptr<faiss::Index> index(faiss::read_index(&reader));
            auto gpu_res = GPUResMgr::GetInstance().GetRes();
            ResScope rs(gpu_res, true);
            gpu_index_ = faiss::gpu::index_cpu_to_gpu(gpu_res->faiss_res_.get(), gpu_res->gpu_id_, index.get());
            res_ = gpu_res;
        } catch (std::exception& e) {
            LOG_KNOWHERE_WARNING_ << "faiss inner error, " << e.what();
            return Status::faiss_inner_error;
        }
        return Status::success;
    }

    virtual std::unique_ptr<BaseConfig>
    CreateConfig() const override {
        return std::make_unique<typename KnowhereConfigType<T>::Type>();
    }

    virtual int64_t
    Dim() const override {
        if (gpu_index_)
            return gpu_index_->d;
        return 0;
    }

    virtual int64_t
    Size() const override {
        return 0;
    }

    virtual int64_t
    Count() const override {
        if (gpu_index_)
            return gpu_index_->ntotal;
        return 0;
    }

    virtual std::string
    Type() const override {
        if constexpr (std::is_same<faiss::IndexIVFFlat, T>::value) {
            return knowhere::IndexEnum::INDEX_FAISS_GPU_IVFFLAT;
        }
        if constexpr (std::is_same<faiss::IndexIVFPQ, T>::value) {
            return knowhere::IndexEnum::INDEX_FAISS_GPU_IVFPQ;
        }
        if constexpr (std::is_same<faiss::IndexIVFScalarQuantizer, T>::value) {
            return knowhere::IndexEnum::INDEX_FAISS_GPU_IVFSQ8;
        }
    }

    virtual ~GpuIvfIndexNode() {
        if (gpu_index_) {
            delete gpu_index_;
        }
    }

 private:
    mutable ResWPtr res_;
    faiss::Index* gpu_index_;
};

KNOWHERE_REGISTER_GLOBAL(GPU_IVF_FLAT, [](const Object& object) {
    return Index<GpuIvfIndexNode<faiss::IndexIVFFlat>>::Create(object);
});
KNOWHERE_REGISTER_GLOBAL(GPU_IVF_PQ, [](const Object& object) {
    return Index<GpuIvfIndexNode<faiss::IndexIVFPQ>>::Create(object);
});
KNOWHERE_REGISTER_GLOBAL(GPU_IVF_SQ8, [](const Object& object) {
    return Index<GpuIvfIndexNode<faiss::IndexIVFScalarQuantizer>>::Create(object);
});

}  // namespace knowhere
