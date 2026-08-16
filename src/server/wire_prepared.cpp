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

#include "server/wire_session_internal.h"

#include <optional>

namespace wire_session_internal {
namespace {
deltakernel::DeltaValueType delta_type(Type type) {
    switch (type) {
    case Type::INT32:
        return deltakernel::DeltaValueType::Int;
    case Type::FLOAT32:
        return deltakernel::DeltaValueType::Float;
    case Type::CHAR:
        return deltakernel::DeltaValueType::Char;
    default:
        throw wire_protocol::ProtocolError("unsupported prepared parameter type");
    }
}

Type protocol_type(deltakernel::DeltaValueType type) {
    switch (type) {
    case deltakernel::DeltaValueType::Int:
        return Type::INT32;
    case deltakernel::DeltaValueType::Float:
        return Type::FLOAT32;
    case deltakernel::DeltaValueType::Char:
        return Type::CHAR;
    }
    throw wire_protocol::ProtocolError("unsupported Delta result type");
}
} // namespace

ParameterFrame make_parameter_frame(const std::vector<Value>& wire_values) {
    std::vector<::Value> values;
    values.reserve(wire_values.size());
    for (const auto& wire_value : wire_values) {
        ::Value value;
        value.type =
            wire_value.type == Type::INT32 ? TYPE_INT : (wire_value.type == Type::FLOAT32 ? TYPE_FLOAT : TYPE_STRING);
        if (!wire_value.present) {
            value.set_null();
            value.type = wire_value.type == Type::INT32 ? TYPE_INT
                                                        : (wire_value.type == Type::FLOAT32 ? TYPE_FLOAT : TYPE_STRING);
        } else if (wire_value.type == Type::INT32) {
            value.set_int(wire_value.int32);
        } else if (wire_value.type == Type::FLOAT32) {
            float number;
            std::memcpy(&number, &wire_value.float_bits, sizeof(number));
            value.set_float(number);
        } else {
            value.set_str(wire_value.text);
        }
        values.push_back(std::move(value));
    }
    return ParameterFrame(std::move(values));
}

std::vector<std::unique_ptr<ast::Value>> make_typed_parameter_nodes(const std::vector<Type>& parameters) {
    std::vector<std::unique_ptr<ast::Value>> typed_parameters;
    typed_parameters.reserve(parameters.size());
    for (std::size_t i = 0; i < parameters.size(); ++i) {
        ast::SvType declared_type;
        switch (parameters[i]) {
        case Type::INT32:
            declared_type = ast::SV_TYPE_INT;
            break;
        case Type::FLOAT32:
            declared_type = ast::SV_TYPE_FLOAT;
            break;
        case Type::CHAR:
            declared_type = ast::SV_TYPE_STRING;
            break;
        default:
            throw wire_protocol::ProtocolError("unsupported prepared parameter type");
        }
        typed_parameters.push_back(std::make_unique<ast::Parameter>(i + 1, declared_type));
    }
    return typed_parameters;
}

PreparedStatement inspect_prepared(DatabaseInstance& database, std::uint16_t id, bool query,
                                   std::vector<Type> parameters, std::unique_ptr<ast::TreeNode> template_tree,
                                   IsolationLevel isolation) {
    auto typed_parameters = make_typed_parameter_nodes(parameters);
    auto bound = ast::clone_bound_tree(*template_tree, typed_parameters);
    if (database.is_delta()) {
        std::vector<deltakernel::DeltaValueType> declared_parameters;
        declared_parameters.reserve(parameters.size());
        for (Type type : parameters)
            declared_parameters.push_back(delta_type(type));
        const auto description = database.delta_database->DescribePrepared(*bound, declared_parameters);
        if (description.query != query)
            throw wire_protocol::ProtocolError("prepared result kind does not match SQL");
        PreparedStatement result;
        result.id = id;
        result.query = query;
        result.template_tree = std::move(template_tree);
        result.parameters = std::move(parameters);
        result.names = description.names;
        result.result_types.reserve(description.types.size());
        for (deltakernel::DeltaValueType type : description.types)
            result.result_types.push_back(protocol_type(type));
        result.catalog_generation = description.catalog_generation;
        result.delta_program = database.delta_database->CompilePrepared(std::move(bound), declared_parameters);
        return result;
    }

    Context context(&database.legacy().lock_manager, &database.legacy().log_manager, nullptr,
                    &database.legacy().txn_manager);
    context.isolation_level_ = isolation;
    std::unique_ptr<Query> query_tree = database.legacy().analyze.do_analyze(std::move(bound));
    std::unique_ptr<Plan> plan = database.legacy().optimizer.plan_query(std::move(query_tree), &context);
    const bool actual_query = plan->tag == T_select;
    if (actual_query != query)
        throw wire_protocol::ProtocolError("prepared result kind does not match SQL");
    PreparedStatement result;
    result.id = id;
    result.query = query;
    result.template_tree = std::move(template_tree);
    result.parameters = std::move(parameters);
    result.database_identity = database.legacy().sm_manager.get_database_identity_under_catalog_guard();
    result.catalog_generation = database.legacy().sm_manager.get_catalog_generation();
    PreparedStatementKind statement_kind = PreparedStatementKind::Unsupported;
    if (plan->tag == T_select) {
        statement_kind = PreparedStatementKind::Select;
    } else if (plan->tag == T_Insert) {
        statement_kind = PreparedStatementKind::Insert;
    } else if (plan->tag == T_Update) {
        statement_kind = PreparedStatementKind::Update;
    }
    if (actual_query) {
        auto [output_names, result_schema] = database.legacy().ql_manager.inspect_select_plan(*plan, &context);
        result.names = output_names;
        for (const auto& column : result_schema) {
            result.result_types.push_back(column.type == TYPE_INT     ? Type::INT32
                                          : column.type == TYPE_FLOAT ? Type::FLOAT32
                                                                      : Type::CHAR);
        }
        result.descriptor = PreparedPlanDescriptor::Build(std::move(plan), statement_kind, std::move(output_names),
                                                          std::move(result_schema), result.database_identity,
                                                          result.catalog_generation);
    } else if (statement_kind != PreparedStatementKind::Unsupported) {
        result.descriptor = PreparedPlanDescriptor::Build(std::move(plan), statement_kind, {}, {},
                                                          result.database_identity, result.catalog_generation);
    }
    return result;
}

void revalidate_prepared(DatabaseInstance& database, PreparedStatement& statement, IsolationLevel isolation) {
    auto typed_parameters = make_typed_parameter_nodes(statement.parameters);
    auto template_tree = ast::clone_bound_tree(*statement.template_tree, typed_parameters);
    PreparedStatement refreshed = inspect_prepared(database, statement.id, statement.query, statement.parameters,
                                                   std::move(template_tree), isolation);
    if (refreshed.query != statement.query || refreshed.names != statement.names ||
        refreshed.result_types != statement.result_types) {
        throw wire_protocol::ProtocolError("prepared result schema changed after catalog update");
    }
    statement = std::move(refreshed);
}

std::vector<std::uint8_t> prepare_set(const std::vector<PreparedStatement>& statements) {
    std::vector<wire_protocol::PreparedSchema> schemas;
    schemas.reserve(statements.size());
    for (const auto& statement : statements) {
        if (statement.names.size() != statement.result_types.size()) {
            throw wire_protocol::ProtocolError("prepared schema name/type count mismatch");
        }
        wire_protocol::PreparedSchema schema;
        schema.statement_id = statement.id;
        schema.columns.reserve(statement.result_types.size());
        for (std::size_t i = 0; i < statement.result_types.size(); ++i) {
            schema.columns.push_back({statement.names[i], statement.result_types[i]});
        }
        schemas.push_back(std::move(schema));
    }
    return wire_protocol::encode_prepare_ok(schemas);
}

void handle_prepare_set(DatabaseInstance& database, int fd, Reader& reader, SessionState& session,
                        std::unordered_map<std::uint16_t, PreparedStatement>& prepared) {
    const auto count = reader.u16();
    if (count == 0 || count > 256) {
        throw wire_protocol::ProtocolError("invalid prepared statement count");
    }
    std::optional<SmManager::CatalogSharedGuard> catalog_guard;
    if (!database.is_delta())
        catalog_guard.emplace(database.legacy().sm_manager.acquire_catalog_shared());
    std::vector<PreparedStatement> pending;
    std::unordered_map<std::uint16_t, bool> ids;
    for (std::uint16_t i = 0; i < count; ++i) {
        PreparedStatement statement;
        statement.id = reader.u16();
        if (statement.id == 0 || ids[statement.id]) {
            throw wire_protocol::ProtocolError("prepared statement ids must be unique and non-zero");
        }
        ids[statement.id] = true;
        const auto result_kind = reader.u8();
        if (result_kind > 1) {
            throw wire_protocol::ProtocolError("invalid prepared result kind");
        }
        statement.query = result_kind == 1;
        const auto parameter_count = reader.u16();
        statement.parameters.reserve(parameter_count);
        for (std::uint16_t p = 0; p < parameter_count; ++p) {
            const auto type = static_cast<Type>(reader.u8());
            if (type != Type::INT32 && type != Type::FLOAT32 && type != Type::CHAR) {
                throw wire_protocol::ProtocolError("unknown prepared parameter type");
            }
            statement.parameters.push_back(type);
        }
        const auto sql_size = reader.u32();
        if (sql_size > reader.remaining() || sql_size > wire_protocol::kMaxPayload) {
            throw wire_protocol::ProtocolError("invalid prepared SQL length");
        }
        const std::string template_sql = reader.bytes(sql_size);
        if (template_sql.empty() || template_sql.find('\0') != std::string::npos || !is_valid_utf8(template_sql)) {
            throw wire_protocol::ProtocolError("prepared SQL must be non-empty UTF-8 without NUL");
        }
        auto template_tree = ast::parse_sql(template_sql);
        if (template_tree == nullptr)
            throw wire_protocol::ProtocolError("empty prepared SQL");
        if (changes_catalog(template_tree->type)) {
            throw wire_protocol::ProtocolError("PREPARE_SET does not allow structural DDL or LOAD");
        }
        std::vector<bool> seen(statement.parameters.size(), false);
        std::function<void(const ast::TreeNode&)> collect = [&](const ast::TreeNode& node) {
            if (node.type == ast::AstType::Parameter) {
                auto ordinal = static_cast<const ast::Parameter&>(node).ordinal;
                if (ordinal == 0 || ordinal > statement.parameters.size())
                    throw wire_protocol::ProtocolError("parameter marker is out of range");
                seen[ordinal - 1] = true;
            }
            if (node.type == ast::AstType::SelectStmt) {
                const auto& select = static_cast<const ast::SelectStmt&>(node);
                if (select.limit_is_parameter) {
                    if (select.limit_parameter == 0 || select.limit_parameter > statement.parameters.size())
                        throw wire_protocol::ProtocolError("parameter marker is out of range");
                    seen[select.limit_parameter - 1] = true;
                }
                if (select.offset_is_parameter) {
                    if (select.offset_parameter == 0 || select.offset_parameter > statement.parameters.size())
                        throw wire_protocol::ProtocolError("parameter marker is out of range");
                    seen[select.offset_parameter - 1] = true;
                }
            }
        };
        std::function<void(const ast::Expr&)> visit_expr = [&](const ast::Expr& expr) {
            if (expr.type == ast::AstType::Parameter) {
                auto ordinal = static_cast<const ast::Parameter&>(expr).ordinal;
                if (ordinal == 0 || ordinal > statement.parameters.size())
                    throw wire_protocol::ProtocolError("parameter marker is out of range");
                seen[ordinal - 1] = true;
            }
        };
        std::function<void(const ast::TreeNode&)> walk = [&](const ast::TreeNode& node) {
            collect(node);
            switch (node.type) {
            case ast::AstType::InsertStmt:
                for (const auto& v : static_cast<const ast::InsertStmt&>(node).vals)
                    visit_expr(*v);
                break;
            case ast::AstType::DeleteStmt:
                for (const auto& c : static_cast<const ast::DeleteStmt&>(node).conds) {
                    visit_expr(*c->lhs);
                    visit_expr(*c->rhs);
                }
                break;
            case ast::AstType::UpdateStmt: {
                const auto& x = static_cast<const ast::UpdateStmt&>(node);
                for (const auto& s : x.set_clauses) {
                    if (s->val)
                        visit_expr(*s->val);
                    for (const auto& term : s->additional_terms)
                        visit_expr(*term.val);
                }
                for (const auto& c : x.conds) {
                    visit_expr(*c->lhs);
                    visit_expr(*c->rhs);
                }
                break;
            }
            case ast::AstType::SelectStmt: {
                const auto& x = static_cast<const ast::SelectStmt&>(node);
                for (const auto& i : x.select_items)
                    visit_expr(*i->expr);
                for (const auto& c : x.conds) {
                    visit_expr(*c->lhs);
                    visit_expr(*c->rhs);
                }
                for (const auto& h : x.having_conds) {
                    visit_expr(*h->lhs);
                    visit_expr(*h->rhs);
                }
                for (const auto& o : x.order_by_items)
                    visit_expr(*o->expr);
                break;
            }
            default:
                break;
            }
        };
        walk(*template_tree);
        for (bool marker_seen : seen)
            if (!marker_seen)
                throw wire_protocol::ProtocolError("parameter markers must be dense");
        statement = inspect_prepared(database, statement.id, statement.query, std::move(statement.parameters),
                                     std::move(template_tree), session.isolation);
        pending.push_back(std::move(statement));
    }
    reader.require_end();
    if (database.is_delta()) {
        if (database.delta_database->CatalogGeneration() != pending.front().catalog_generation)
            throw wire_protocol::ProtocolError("catalog changed during PREPARE_SET");
    } else if (database.legacy().sm_manager.get_catalog_generation() != pending.front().catalog_generation ||
               database.legacy().sm_manager.get_database_identity_under_catalog_guard() !=
                   pending.front().database_identity)
        throw wire_protocol::ProtocolError("catalog changed during PREPARE_SET");
    const auto response = prepare_set(pending);
    std::unordered_map<std::uint16_t, PreparedStatement> replacement;
    replacement.reserve(pending.size());
    for (auto& statement : pending) {
        replacement.emplace(statement.id, std::move(statement));
    }
    prepared.swap(replacement);
    wire_protocol::write_frame(fd, Tag::PREPARE_OK, response);
}

} // namespace wire_session_internal
