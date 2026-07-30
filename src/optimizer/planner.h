/* Copyright (c) 2023 Renmin University of China
   Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once

#include <atomic>
#include <cassert>
#include <cstdint>
#include <cstring>
#include <list>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "execution/execution_defs.h"
#include "execution/execution_manager.h"
#include "record/rm.h"
#include "system/sm.h"
#include "common/context.h"
#include "plan.h"
#include "parser/parser.h"
#include "common/common.h"
#include "analyze/analyze.h"

class Planner {
private:
    SmManager* sm_manager_;

    struct PhysicalPlanTemplate {
        struct ScanDecision {
            PlanTag tag = T_SeqScan;
            std::vector<std::string> index_col_names;
        };

        struct JoinDecision {
            bool use_inlj = false;
            TabCol inlj_left_col;
            TabCol inlj_right_col;
            std::vector<std::string> inlj_index_col_names;
        };

        std::vector<std::string> ordered_tables;
        std::vector<ScanDecision> scan_decisions;
        std::vector<JoinDecision> join_decisions;
    };

    using PhysicalPlanCacheLru = std::list<std::string>;

    struct PhysicalPlanCacheEntry {
        std::shared_ptr<const PhysicalPlanTemplate> plan_template;
        PhysicalPlanCacheLru::iterator lru_position;
    };

    static constexpr size_t kPhysicalPlanCacheCapacity = 256;
    // Hits update LRU recency, so lookup and insert/evict operations take the
    // writer lock while immutable plan templates remain shared across callers.
    mutable std::shared_mutex physical_plan_cache_latch_;
    PhysicalPlanCacheLru physical_plan_cache_lru_;
    std::unordered_map<std::string, PhysicalPlanCacheEntry> physical_plan_cache_;
    std::uint64_t physical_plan_cache_generation_ = 0;
    std::atomic<std::uint64_t> physical_plan_cache_hits_{0};
    std::atomic<std::uint64_t> physical_plan_cache_misses_{0};

    struct PointProgramCacheEntry {
        CompiledPointProgramPtr program;
        std::list<std::string>::iterator lru_position;
    };

    static constexpr size_t kCompiledPointProgramCacheCapacity = 256;
    mutable std::shared_mutex point_program_cache_latch_;
    std::list<std::string> point_program_cache_lru_;
    std::unordered_map<std::string, PointProgramCacheEntry> point_program_cache_;
    std::uint64_t point_program_cache_generation_ = 0;
    std::atomic<std::uint64_t> point_program_cache_hits_{0};
    std::atomic<std::uint64_t> point_program_cache_misses_{0};

public:
    Planner(SmManager* sm_manager) : sm_manager_(sm_manager) {}

    std::unique_ptr<Plan> do_planner(std::unique_ptr<Query> query, Context* context);

private:
    std::unique_ptr<Query> logical_optimization(std::unique_ptr<Query> query, Context* context);
    std::unique_ptr<Plan> physical_optimization(Query* query, Context* context);
    std::string make_physical_plan_cache_key(const Query& query, std::uint64_t catalog_generation) const;
    PhysicalPlanTemplate build_physical_plan_template(const Query& query);
    std::unique_ptr<Plan> instantiate_physical_plan(const Query& query, const PhysicalPlanTemplate& plan_template);
    std::string make_point_program_cache_key(PointProgramKind kind, const std::string& tab_name,
                                             const PointAccessPath& point_access,
                                             const std::vector<Condition>& conditions,
                                             const std::vector<SetClause>& set_clauses,
                                             std::uint64_t catalog_generation) const;
    CompiledPointProgramPtr build_compiled_point_program(PointProgramKind kind, const std::string& tab_name,
                                                         const PointAccessPath& point_access,
                                                         const std::vector<Condition>& conditions,
                                                         const std::vector<SetClause>& set_clauses,
                                                         std::uint64_t catalog_generation) const;
    std::optional<CompiledPointProgramPtr> find_compiled_point_program(const std::string& key,
                                                                       std::uint64_t catalog_generation);
    void cache_compiled_point_program(std::string key, std::uint64_t catalog_generation,
                                      CompiledPointProgramPtr program);
    std::optional<std::shared_ptr<const PhysicalPlanTemplate>>
    find_physical_plan_template(const std::string& key, std::uint64_t catalog_generation);
    void cache_physical_plan_template(std::string key, std::uint64_t catalog_generation,
                                      PhysicalPlanTemplate plan_template);

    std::unique_ptr<Plan> generate_sort_plan(const Query* query, std::unique_ptr<Plan> plan);
    std::unique_ptr<Plan> generate_limit_plan(const Query* query, std::unique_ptr<Plan> plan);

    std::unique_ptr<Plan> generate_select_plan(std::unique_ptr<Query> query, Context* context);
    std::unique_ptr<Plan> generate_union_plan(std::unique_ptr<Query> query, Context* context);

    // int get_indexNo(std::string tab_name, std::vector<Condition> curr_conds);
    bool get_index_cols(std::string tab_name, std::vector<Condition>& curr_conds,
                        std::vector<std::string>& index_col_names);
    bool get_skip_scan_index_cols(std::string tab_name, std::vector<Condition>& curr_conds,
                                  std::vector<std::string>& index_col_names);
    PlanTag choose_scan_plan_tag(std::string tab_name, std::vector<Condition>& curr_conds,
                                 std::vector<std::string>& index_col_names);

    ColType interp_sv_type(ast::SvType sv_type) {
        std::map<ast::SvType, ColType> m = {{ast::SV_TYPE_INT, TYPE_INT},
                                            {ast::SV_TYPE_FLOAT, TYPE_FLOAT},
                                            {ast::SV_TYPE_STRING, TYPE_STRING},
                                            {ast::SV_TYPE_DATETIME, TYPE_DATETIME}};
        return m.at(sv_type);
    }
};
