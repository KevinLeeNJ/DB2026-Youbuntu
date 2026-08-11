/* Copyright (c) 2026 Team Youbuntu
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once

#include <memory>

class AbstractExecutor;
class Context;
class ParameterFrame;
class Plan;
class SmManager;

std::unique_ptr<AbstractExecutor> BuildExecutorTree(const Plan& plan, SmManager& sm_manager, Context* context,
                                                    const ParameterFrame* parameters = nullptr,
                                                    bool count_rows = false);
