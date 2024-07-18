/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include <algorithm>
#include "sql/operator/group_by_vec_physical_operator.h"
#include "sql/expr/aggregate_state.h"

GroupByVecPhysicalOperator::GroupByVecPhysicalOperator(
  std::vector<std::unique_ptr<Expression>> &&group_by_exprs, std::vector<Expression *> &&expressions): hash_table_(expressions) {
  group_by_exprs_ = std::move(group_by_exprs);
  aggregate_expressions_ = std::move(expressions);
  value_expressions_.reserve(aggregate_expressions_.size());
  std::ranges::for_each(aggregate_expressions_, [this](Expression *expr) {
    auto *aggregate_expr = static_cast<AggregateExpr *>(expr);
    Expression *child_expr = aggregate_expr->child().get();
    ASSERT(child_expr != nullptr, "aggregate expression must have a child expression");
    value_expressions_.emplace_back(child_expr);
  });
  auto group_size = group_by_exprs_.size();
  for (size_t i = 0; i < group_size; i++) {
    auto expr = group_by_exprs_[i].get();
    switch (expr->value_type()) {
      case AttrType::INTS:
        output_chunk_.add_column(make_unique<Column>(AttrType::INTS, sizeof(int)), i);
        break;
      case AttrType::FLOATS:
        output_chunk_.add_column(make_unique<Column>(AttrType::FLOATS, sizeof(float)), i);
        break;
      case AttrType::CHARS:
        output_chunk_.add_column(make_unique<Column>(AttrType::CHARS, expr->value_length()), i);
        break;
      case AttrType::BOOLEANS:
        output_chunk_.add_column(make_unique<Column>(AttrType::BOOLEANS, sizeof(bool)), i);
        break;
      default:
        ASSERT(false, "not supported value type");
    }
  }
  for (size_t i = 0; i < aggregate_expressions_.size(); i++) {
    auto &expr = aggregate_expressions_[i];
    ASSERT(expr->type() == ExprType::AGGREGATION, "expected an aggregation expression");
    auto *aggregate_expr = static_cast<AggregateExpr *>(expr);
    switch (aggregate_expr->value_type()) {
      case AttrType::INTS:
        output_chunk_.add_column(make_unique<Column>(AttrType::INTS, sizeof(int)), i + group_size);
        break;
      case AttrType::FLOATS:
        output_chunk_.add_column(make_unique<Column>(AttrType::FLOATS, sizeof(float)), i + group_size);
        break;
      default:
        ASSERT(false, "not supported aggregation type");
    }
  }
}

RC GroupByVecPhysicalOperator::open(Trx* trx) {
  ASSERT(children_.size() == 1, "group by operator only support one child, but got %d", children_.size());

  PhysicalOperator &child = *children_[0];
  RC                rc    = child.open(trx);
  if (OB_FAIL(rc)) {
    LOG_INFO("failed to open child operator. rc=%s", strrc(rc));
    return rc;
  }
  while (OB_SUCC(rc = child.next(chunk_))) {
    Chunk group_chunk;
    Chunk aggr_chunk;
    for (auto i = 0;i < group_by_exprs_.size();i++) {
      auto to_add = make_unique<Column>();
      group_by_exprs_[i]->get_column(chunk_, *to_add);
      group_chunk.add_column(std::move(to_add), i);
    }
    for (auto i = 0;i < aggregate_expressions_.size();i++) {
      auto to_add = make_unique<Column>();
      aggregate_expressions_[i]->get_column(chunk_, *to_add);
      aggr_chunk.add_column(std::move(to_add), i);
    }
    hash_table_.add_chunk(group_chunk, aggr_chunk);
  }
  scanner_ = new StandardAggregateHashTable::Scanner(&hash_table_);
  scanner_->open_scan();
  if (rc == RC::RECORD_EOF) {
    rc = RC::SUCCESS;
  }
  return rc;
}

RC GroupByVecPhysicalOperator::next(Chunk &chunk) {
  RC rc = scanner_->next(output_chunk_);
  if (OB_FAIL(rc)) {
    return rc;
  }
  chunk.reference(output_chunk_);
  return rc;
}

RC GroupByVecPhysicalOperator::close() {
  children_[0]->close();
  scanner_->close_scan();
  return RC::SUCCESS;
}