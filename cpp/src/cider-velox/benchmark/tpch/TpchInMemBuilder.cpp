/*
 * Copyright(c) 2022-2023 Intel Corporation.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
#include "TpchInMemBuilder.h"

#include "velox/common/base/tests/Fs.h"
#include "velox/dwio/common/ReaderFactory.h"
#include "velox/tpch/gen/TpchGen.h"

using facebook::velox::tpch::Table;

namespace facebook::velox::exec::test {

namespace {

/// Return the Date filter expression as per data format.
std::string formatDateFilter(const std::string& stringDate,
                             const std::string& lowerBound,
                             const std::string& upperBound) {
  auto suffix = "::DATE";

  if (!lowerBound.empty() && !upperBound.empty()) {
    return fmt::format(
        "{} between {}{} and {}{}", stringDate, lowerBound, suffix, upperBound, suffix);
  } else if (!lowerBound.empty()) {
    return fmt::format("{} > {}{}", stringDate, lowerBound, suffix);
  } else if (!upperBound.empty()) {
    return fmt::format("{} < {}{}", stringDate, upperBound, suffix);
  }

  VELOX_FAIL("Date range check expression must have either a lower or an upper bound");
}

std::vector<std::string> mergeColumnNames(
    const std::vector<std::string>& firstColumnVector,
    const std::vector<std::string>& secondColumnVector) {
  std::vector<std::string> mergedColumnVector = std::move(firstColumnVector);
  mergedColumnVector.insert(
      mergedColumnVector.end(), secondColumnVector.begin(), secondColumnVector.end());
  return mergedColumnVector;
}
}  // namespace

TpchPlan TpchInMemBuilder::getWarmupPlan(double scaleFactor) const {
  std::vector<std::string> selectedColumns = {"l_orderkey",
                                              "l_suppkey",
                                              "l_linenumber",
                                              "l_returnflag",
                                              "l_linestatus",
                                              "l_quantity",
                                              "l_extendedprice",
                                              "l_discount",
                                              "l_tax",
                                              "l_shipdate"};

  core::PlanNodeId lineitemPlanNodeId;

  auto plan = PlanBuilder()
                  .tableScan(Table::TBL_LINEITEM, std::move(selectedColumns), scaleFactor)
                  .capturePlanNodeId(lineitemPlanNodeId)
                  .partialAggregation({}, {"count(0)"})
                  .localPartition({})
                  .finalAggregation()
                  .planNode();

  TpchPlan context;
  context.plan = std::move(plan);
  context.scanNodes.emplace(lineitemPlanNodeId);
  return context;
}

TpchPlan TpchInMemBuilder::getQueryPlan(int queryId, double scaleFactor) const {
  switch (queryId) {
    case 1:
      return getQ1Plan(scaleFactor);
    case 3:
      return getQ3Plan(scaleFactor);
    case 5:
      return getQ5Plan(scaleFactor);
    case 6:
      return getQ6Plan(scaleFactor);
    case 7:
      return getQ7Plan(scaleFactor);
    case 8:
      return getQ8Plan(scaleFactor);
    case 9:
      return getQ9Plan(scaleFactor);
    case 10:
      return getQ10Plan(scaleFactor);
    case 12:
      return getQ12Plan(scaleFactor);
    case 13:
      return getQ13Plan(scaleFactor);
    case 14:
      return getQ14Plan(scaleFactor);
    case 15:
      return getQ15Plan(scaleFactor);
    case 16:
      return getQ16Plan(scaleFactor);
    case 18:
      return getQ18Plan(scaleFactor);
    case 19:
      return getQ19Plan(scaleFactor);
    case 22:
      return getQ22Plan(scaleFactor);
    case 23:
      return getQ23Plan(scaleFactor);
    default:
      VELOX_NYI("TPC-H query {} is not supported yet", queryId);
  }
}

TpchPlan TpchInMemBuilder::getQ1Plan(double scaleFactor) const {
  std::vector<std::string> selectedColumns = {"l_orderkey",
                                              "l_suppkey",
                                              "l_linenumber",
                                              "l_returnflag",
                                              "l_linestatus",
                                              "l_quantity",
                                              "l_extendedprice",
                                              "l_discount",
                                              "l_tax",
                                              "l_shipdate"};

  //   const auto selectedRowType = getRowType(kLineitem, selectedColumns);
  //   const auto& fileColumnNames = getFileColumnNames(kLineitem);

  // shipdate <= '1998-09-02'
  const auto shipDate = "l_shipdate";
  auto filter = formatDateFilter(shipDate, "", "'1998-09-03'");

  core::PlanNodeId lineitemPlanNodeId;

  auto plan =
      PlanBuilder()
          //   .tableScan(kLineitem, selectedRowType, fileColumnNames, {filter})
          .tableScan(Table::TBL_LINEITEM, std::move(selectedColumns), scaleFactor)
          .capturePlanNodeId(lineitemPlanNodeId)
          //   .filter(filter)
          .project(
              {"l_returnflag",
               "l_linestatus",
               "l_quantity",
               "l_extendedprice",
               "l_extendedprice * (1.0 - l_discount) AS l_sum_disc_price",
               "l_extendedprice * (1.0 - l_discount) * (1.0 + l_tax) AS l_sum_charge",
               "l_discount"})
          .partialAggregation({"l_returnflag", "l_linestatus"},
                              {"sum(l_quantity)",
                               "sum(l_extendedprice)",
                               "sum(l_sum_disc_price)",
                               "sum(l_sum_charge)",
                               "avg(l_quantity)",
                               "avg(l_extendedprice)",
                               "avg(l_discount)",
                               "count(0)"})
          .localPartition({})
          .finalAggregation()
          .orderBy({"l_returnflag", "l_linestatus"}, false)
          .planNode();

  TpchPlan context;
  context.plan = std::move(plan);
  context.scanNodes.emplace(lineitemPlanNodeId);
  return context;
}

TpchPlan TpchInMemBuilder::getQ3Plan(double scaleFactor) const {
  std::vector<std::string> lineitemColumns = {
      "l_shipdate", "l_orderkey", "l_extendedprice", "l_discount"};
  std::vector<std::string> ordersColumns = {
      "o_orderdate", "o_shippriority", "o_custkey", "o_orderkey"};
  std::vector<std::string> customerColumns = {"c_custkey", "c_mktsegment"};

  //   const auto lineitemSelectedRowType = getRowType(kLineitem, lineitemColumns);
  //   const auto& lineitemFileColumns = getFileColumnNames(kLineitem);
  //   const auto ordersSelectedRowType = getRowType(kOrders, ordersColumns);
  //   const auto& ordersFileColumns = getFileColumnNames(kOrders);
  //   const auto customerSelectedRowType = getRowType(kCustomer, customerColumns);
  //   const auto& customerFileColumns = getFileColumnNames(kCustomer);

  const auto orderDate = "o_orderdate";
  const auto shipDate = "l_shipdate";
  auto orderDateFilter = formatDateFilter(orderDate, "", "'1995-03-15'");
  auto shipDateFilter = formatDateFilter(shipDate, "'1995-03-15'", "");
  auto customerFilter = "c_mktsegment = 'BUILDING'";

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId lineitemPlanNodeId;
  core::PlanNodeId ordersPlanNodeId;
  core::PlanNodeId customerPlanNodeId;

  auto customers =
      PlanBuilder(planNodeIdGenerator)
          //   .tableScan(
          //       kCustomer, customerSelectedRowType, customerFileColumns,
          //       {customerFilter})
          .tableScan(Table::TBL_CUSTOMER, std::move(customerColumns), scaleFactor)
          .capturePlanNodeId(customerPlanNodeId)
          //   .filter(customerFilter)
          .planNode();

  auto custkeyJoinNode =
      PlanBuilder(planNodeIdGenerator)
          //   .tableScan(kOrders, ordersSelectedRowType, ordersFileColumns,
          //   {orderDateFilter})
          .tableScan(Table::TBL_ORDERS, std::move(ordersColumns), scaleFactor)
          .capturePlanNodeId(ordersPlanNodeId)
          //   .filter(orderDateFilter)
          .hashJoin({"o_custkey"},
                    {"c_custkey"},
                    customers,
                    "",
                    {"o_orderdate", "o_shippriority", "o_orderkey"})
          .planNode();

  auto plan =
      PlanBuilder(planNodeIdGenerator)
          //   .tableScan(
          //       kLineitem, lineitemSelectedRowType, lineitemFileColumns,
          //       {shipDateFilter})
          .tableScan(Table::TBL_LINEITEM, std::move(lineitemColumns), scaleFactor)
          .capturePlanNodeId(lineitemPlanNodeId)
          //   .filter(shipDateFilter)
          .project({"l_extendedprice * (1.0 - l_discount) AS part_revenue", "l_orderkey"})
          .hashJoin({"l_orderkey"},
                    {"o_orderkey"},
                    custkeyJoinNode,
                    "",
                    {"l_orderkey", "o_orderdate", "o_shippriority", "part_revenue"})
          .partialAggregation({"l_orderkey", "o_orderdate", "o_shippriority"},
                              {"sum(part_revenue) as revenue"})
          .localPartition({})
          .finalAggregation()
          .project({"l_orderkey", "revenue", "o_orderdate", "o_shippriority"})
          .orderBy({"revenue DESC", "o_orderdate"}, false)
          .limit(0, 10, false)
          .planNode();

  TpchPlan context;
  context.plan = std::move(plan);
  context.scanNodes.emplace(lineitemPlanNodeId);
  context.scanNodes.emplace(ordersPlanNodeId);
  context.scanNodes.emplace(customerPlanNodeId);
  return context;
}

TpchPlan TpchInMemBuilder::getQ5Plan(double scaleFactor) const {
  std::vector<std::string> customerColumns = {"c_custkey", "c_nationkey"};
  std::vector<std::string> ordersColumns = {"o_orderdate", "o_custkey", "o_orderkey"};
  std::vector<std::string> lineitemColumns = {
      "l_suppkey", "l_orderkey", "l_discount", "l_extendedprice"};
  std::vector<std::string> supplierColumns = {"s_nationkey", "s_suppkey"};
  std::vector<std::string> nationColumns = {"n_nationkey", "n_name", "n_regionkey"};
  std::vector<std::string> regionColumns = {"r_regionkey", "r_name"};

  //   auto customerSelectedRowType = getRowType(kCustomer, customerColumns);
  //   const auto& customerFileColumns = getFileColumnNames(kCustomer);
  //   auto ordersSelectedRowType = getRowType(kOrders, ordersColumns);
  //   const auto& ordersFileColumns = getFileColumnNames(kOrders);
  //   auto lineitemSelectedRowType = getRowType(kLineitem, lineitemColumns);
  //   const auto& lineitemFileColumns = getFileColumnNames(kLineitem);
  //   auto supplierSelectedRowType = getRowType(kSupplier, supplierColumns);
  //   const auto& supplierFileColumns = getFileColumnNames(kSupplier);
  //   auto nationSelectedRowType = getRowType(kNation, nationColumns);
  //   const auto& nationFileColumns = getFileColumnNames(kNation);
  //   auto regionSelectedRowType = getRowType(kRegion, regionColumns);
  //   const auto& regionFileColumns = getFileColumnNames(kRegion);

  std::string regionNameFilter = "r_name = 'ASIA'";
  const auto orderDate = "o_orderdate";
  std::string orderDateFilter =
      formatDateFilter(orderDate, "'1994-01-01'", "'1994-12-31'");

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId customerScanNodeId;
  core::PlanNodeId ordersScanNodeId;
  core::PlanNodeId lineitemScanNodeId;
  core::PlanNodeId supplierScanNodeId;
  core::PlanNodeId nationScanNodeId;
  core::PlanNodeId regionScanNodeId;

  auto region =
      PlanBuilder(planNodeIdGenerator)
          //   .tableScan(
          //       kRegion, regionSelectedRowType, regionFileColumns, {regionNameFilter})
          .tableScan(Table::TBL_REGION, std::move(regionColumns), scaleFactor)
          .capturePlanNodeId(regionScanNodeId)
          //   .filter(regionNameFilter)
          .planNode();

  auto orders = PlanBuilder(planNodeIdGenerator)
                    //   .tableScan(kOrders, ordersSelectedRowType, ordersFileColumns,
                    //   {orderDateFilter})
                    .tableScan(Table::TBL_ORDERS, std::move(ordersColumns), scaleFactor)
                    .capturePlanNodeId(ordersScanNodeId)
                    // .filter(orderDateFilter)
                    .planNode();

  auto customer =
      PlanBuilder(planNodeIdGenerator)
          //   .tableScan(kCustomer, customerSelectedRowType, customerFileColumns)
          .tableScan(Table::TBL_CUSTOMER, std::move(customerColumns), scaleFactor)
          .capturePlanNodeId(customerScanNodeId)
          .planNode();

  auto nationJoinRegion =
      PlanBuilder(planNodeIdGenerator)
          //   .tableScan(kNation, nationSelectedRowType, nationFileColumns)
          .tableScan(Table::TBL_NATION, std::move(nationColumns), scaleFactor)
          .capturePlanNodeId(nationScanNodeId)
          .hashJoin(
              {"n_regionkey"}, {"r_regionkey"}, region, "", {"n_nationkey", "n_name"})
          .planNode();

  auto supplierJoinNationRegion =
      PlanBuilder(planNodeIdGenerator)
          //   .tableScan(kSupplier, supplierSelectedRowType, supplierFileColumns)
          .tableScan(Table::TBL_SUPPLIER, std::move(supplierColumns), scaleFactor)
          .capturePlanNodeId(supplierScanNodeId)
          .hashJoin({"s_nationkey"},
                    {"n_nationkey"},
                    nationJoinRegion,
                    "",
                    {"s_suppkey", "n_name", "s_nationkey"})
          .planNode();

  auto plan = PlanBuilder(planNodeIdGenerator)
                  //   .tableScan(kLineitem, lineitemSelectedRowType, lineitemFileColumns)
                  .tableScan(Table::TBL_LINEITEM, std::move(lineitemColumns), scaleFactor)
                  .capturePlanNodeId(lineitemScanNodeId)
                  .project({"l_extendedprice * (1.0 - l_discount) AS part_revenue",
                            "l_orderkey",
                            "l_suppkey"})
                  .hashJoin({"l_suppkey"},
                            {"s_suppkey"},
                            supplierJoinNationRegion,
                            "",
                            {"n_name", "part_revenue", "s_nationkey", "l_orderkey"})
                  .hashJoin({"l_orderkey"},
                            {"o_orderkey"},
                            orders,
                            "",
                            {"n_name", "part_revenue", "s_nationkey", "o_custkey"})
                  .hashJoin({"s_nationkey", "o_custkey"},
                            {"c_nationkey", "c_custkey"},
                            customer,
                            "",
                            {"n_name", "part_revenue"})
                  .partialAggregation({"n_name"}, {"sum(part_revenue) as revenue"})
                  .localPartition({})
                  .finalAggregation()
                  .orderBy({"revenue DESC"}, false)
                  .project({"n_name", "revenue"})
                  .planNode();

  TpchPlan context;
  context.plan = std::move(plan);
  context.scanNodes.emplace(customerScanNodeId);
  context.scanNodes.emplace(ordersScanNodeId);
  context.scanNodes.emplace(lineitemScanNodeId);
  context.scanNodes.emplace(supplierScanNodeId);
  context.scanNodes.emplace(nationScanNodeId);
  context.scanNodes.emplace(regionScanNodeId);
  return context;
}

TpchPlan TpchInMemBuilder::getQ6Plan(double scaleFactor) const {
  std::vector<std::string> selectedColumns = {
      "l_shipdate", "l_extendedprice", "l_quantity", "l_discount"};

  //   const auto selectedRowType = getRowType(kLineitem, selectedColumns);
  //   const auto& fileColumnNames = getFileColumnNames(kLineitem);

  const auto shipDate = "l_shipdate";
  auto shipDateFilter = formatDateFilter(shipDate, "'1994-01-01'", "'1994-12-31'");

  core::PlanNodeId lineitemPlanNodeId;
  auto plan = PlanBuilder()
                  //   .tableScan(
                  //       kLineitem,
                  //       selectedRowType,
                  //       fileColumnNames,
                  //       {shipDateFilter, "l_discount between 0.05 and 0.07",
                  //       "l_quantity < 24.0"})
                  .tableScan(Table::TBL_LINEITEM, std::move(selectedColumns), scaleFactor)
                  .capturePlanNodeId(lineitemPlanNodeId)
                  //   .filter(shipDateFilter)
                  //   .filter("l_discount between 0.05 and 0.07")
                  //   .filter("l_quantity < 24.0")
                  .project({"l_extendedprice * l_discount"})
                  .partialAggregation({}, {"sum(p0)"})
                  .localPartition({})
                  .finalAggregation()
                  .planNode();
  TpchPlan context;
  context.plan = std::move(plan);
  context.scanNodes.emplace(lineitemPlanNodeId);
  return context;
}

TpchPlan TpchInMemBuilder::getQ7Plan(double scaleFactor) const {
  std::vector<std::string> supplierColumns = {"s_nationkey", "s_suppkey"};
  std::vector<std::string> lineitemColumns = {
      "l_shipdate", "l_suppkey", "l_orderkey", "l_discount", "l_extendedprice"};
  std::vector<std::string> ordersColumns = {"o_custkey", "o_orderkey"};
  std::vector<std::string> customerColumns = {"c_custkey", "c_nationkey"};
  std::vector<std::string> nationColumns = {"n_nationkey", "n_name"};
  std::vector<std::string> nationColumns1 = {"n_nationkey", "n_name"};

  //   auto supplierSelectedRowType = getRowType(kSupplier, supplierColumns);
  //   const auto& supplierFileColumns = getFileColumnNames(kSupplier);
  //   auto lineitemSelectedRowType = getRowType(kLineitem, lineitemColumns);
  //   const auto& lineitemFileColumns = getFileColumnNames(kLineitem);
  //   auto ordersSelectedRowType = getRowType(kOrders, ordersColumns);
  //   const auto& ordersFileColumns = getFileColumnNames(kOrders);
  //   auto customerSelectedRowType = getRowType(kCustomer, customerColumns);
  //   const auto& customerFileColumns = getFileColumnNames(kCustomer);
  //   auto nationSelectedRowType = getRowType(kNation, nationColumns);
  //   const auto& nationFileColumns = getFileColumnNames(kNation);

  const std::string nationFilter = "n_name IN ('FRANCE', 'GERMANY')";
  auto shipDateFilter = formatDateFilter("l_shipdate", "'1995-01-01'", "'1996-12-31'");

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId supplierScanNodeId;
  core::PlanNodeId lineitemScanNodeId;
  core::PlanNodeId ordersScanNodeId;
  core::PlanNodeId customerScanNodeId;
  core::PlanNodeId suppNationScanNodeId;
  core::PlanNodeId custNationScanNodeId;

  auto custNation =
      PlanBuilder(planNodeIdGenerator, pool_.get())
          //   .tableScan(kNation, nationSelectedRowType, nationFileColumns,
          //   {nationFilter})
          .tableScan(Table::TBL_NATION, std::move(nationColumns), scaleFactor)
          .capturePlanNodeId(custNationScanNodeId)
          //   .filter(nationFilter)
          .planNode();

  auto customerJoinNation =
      PlanBuilder(planNodeIdGenerator)
          //   .tableScan(kCustomer, customerSelectedRowType, customerFileColumns)
          .tableScan(Table::TBL_CUSTOMER, std::move(customerColumns), scaleFactor)
          .capturePlanNodeId(customerScanNodeId)
          .hashJoin(
              {"c_nationkey"}, {"n_nationkey"}, custNation, "", {"n_name", "c_custkey"})
          .project({"n_name as cust_nation", "c_custkey"})
          .planNode();

  auto ordersJoinCustomer =
      PlanBuilder(planNodeIdGenerator)
          //   .tableScan(kOrders, ordersSelectedRowType, ordersFileColumns)
          .tableScan(Table::TBL_ORDERS, std::move(ordersColumns), scaleFactor)
          .capturePlanNodeId(ordersScanNodeId)
          .hashJoin({"o_custkey"},
                    {"c_custkey"},
                    customerJoinNation,
                    "",
                    {"cust_nation", "o_orderkey"})
          .planNode();

  auto suppNation =
      PlanBuilder(planNodeIdGenerator, pool_.get())
          //   .tableScan(kNation, nationSelectedRowType, nationFileColumns,
          //   {nationFilter})
          .tableScan(Table::TBL_NATION, std::move(nationColumns1), scaleFactor)
          .capturePlanNodeId(suppNationScanNodeId)
          //   .filter(nationFilter)
          .planNode();

  auto supplierJoinNation =
      PlanBuilder(planNodeIdGenerator)
          //   .tableScan(kSupplier, supplierSelectedRowType, supplierFileColumns)
          .tableScan(Table::TBL_SUPPLIER, std::move(supplierColumns), scaleFactor)
          .capturePlanNodeId(supplierScanNodeId)
          .hashJoin(
              {"s_nationkey"}, {"n_nationkey"}, suppNation, "", {"n_name", "s_suppkey"})
          .project({"n_name as supp_nation", "s_suppkey"})
          .planNode();

  auto plan =
      PlanBuilder(planNodeIdGenerator)
          //   .tableScan(
          //       kLineitem, lineitemSelectedRowType, lineitemFileColumns,
          //       {shipDateFilter})
          .tableScan(Table::TBL_LINEITEM, std::move(lineitemColumns), scaleFactor)
          .capturePlanNodeId(lineitemScanNodeId)
          //   .filter(shipDateFilter)
          .hashJoin({"l_suppkey"},
                    {"s_suppkey"},
                    supplierJoinNation,
                    "",
                    {"supp_nation",
                     "l_extendedprice",
                     "l_discount",
                     "l_shipdate",
                     "l_orderkey"})
          .hashJoin({"l_orderkey"},
                    {"o_orderkey"},
                    ordersJoinCustomer,
                    "(((cust_nation = 'FRANCE') AND (supp_nation = 'GERMANY')) OR "
                    "((cust_nation = 'GERMANY') AND (supp_nation = 'FRANCE')))",
                    {"supp_nation",
                     "cust_nation",
                     "l_extendedprice",
                     "l_discount",
                     "l_shipdate"})
          .project({"cust_nation",
                    "supp_nation",
                    "l_extendedprice * (1.0 - l_discount) as part_revenue",
                    "year(l_shipdate) as l_year"})
          .partialAggregation({"supp_nation", "cust_nation", "l_year"},
                              {"sum(part_revenue) as revenue"})
          .localPartition({})
          .finalAggregation()
          .orderBy({"supp_nation", "cust_nation", "l_year"}, false)
          .planNode();

  TpchPlan context;
  context.plan = std::move(plan);
  context.scanNodes.emplace(customerScanNodeId);
  context.scanNodes.emplace(ordersScanNodeId);
  context.scanNodes.emplace(lineitemScanNodeId);
  context.scanNodes.emplace(supplierScanNodeId);
  context.scanNodes.emplace(suppNationScanNodeId);
  context.scanNodes.emplace(custNationScanNodeId);
  return context;
}

TpchPlan TpchInMemBuilder::getQ8Plan(double scaleFactor) const {
  std::vector<std::string> partColumns = {"p_partkey", "p_type"};
  std::vector<std::string> supplierColumns = {"s_suppkey", "s_nationkey"};
  std::vector<std::string> lineitemColumns = {
      "l_suppkey", "l_orderkey", "l_partkey", "l_extendedprice", "l_discount"};
  std::vector<std::string> ordersColumns = {"o_orderdate", "o_orderkey", "o_custkey"};
  std::vector<std::string> customerColumns = {"c_nationkey", "c_custkey"};
  std::vector<std::string> nationColumns = {"n_nationkey", "n_regionkey"};
  std::vector<std::string> nationColumnsWithName = {
      "n_name", "n_nationkey", "n_regionkey"};
  std::vector<std::string> regionColumns = {"r_name", "r_regionkey"};

  //   auto partSelectedRowType = getRowType(kPart, partColumns);
  //   const auto& partFileColumns = getFileColumnNames(kPart);
  //   auto supplierSelectedRowType = getRowType(kSupplier, supplierColumns);
  //   const auto& supplierFileColumns = getFileColumnNames(kSupplier);
  //   const auto lineitemSelectedRowType = getRowType(kLineitem, lineitemColumns);
  //   const auto& lineitemFileColumns = getFileColumnNames(kLineitem);
  //   const auto ordersSelectedRowType = getRowType(kOrders, ordersColumns);
  //   const auto& ordersFileColumns = getFileColumnNames(kOrders);
  //   const auto customerSelectedRowType = getRowType(kCustomer, customerColumns);
  //   const auto& customerFileColumns = getFileColumnNames(kCustomer);
  //   const auto nationSelectedRowType = getRowType(kNation, nationColumns);
  //   const auto& nationFileColumns = getFileColumnNames(kNation);
  //   const auto nationSelectedRowTypeWithName = getRowType(kNation,
  //   nationColumnsWithName); const auto& nationFileColumnsWithName =
  //   getFileColumnNames(kNation); const auto regionSelectedRowType = getRowType(kRegion,
  //   regionColumns); const auto& regionFileColumns = getFileColumnNames(kRegion);

  const auto orderDateFilter =
      formatDateFilter("o_orderdate", "'1995-01-01'", "'1996-12-31'");

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId partScanNodeId;
  core::PlanNodeId supplierScanNodeId;
  core::PlanNodeId lineitemScanNodeId;
  core::PlanNodeId ordersScanNodeId;
  core::PlanNodeId customerScanNodeId;
  core::PlanNodeId nationScanNodeId;
  core::PlanNodeId nationScanNodeIdWithName;
  core::PlanNodeId regionScanNodeId;

  auto nationWithName =
      PlanBuilder(planNodeIdGenerator)
          //   .tableScan(kNation, nationSelectedRowTypeWithName,
          //   nationFileColumnsWithName)
          .tableScan(Table::TBL_NATION, std::move(nationColumnsWithName), scaleFactor)
          .capturePlanNodeId(nationScanNodeIdWithName)
          .planNode();

  auto region = PlanBuilder(planNodeIdGenerator)
                    //   .tableScan(
                    //       kRegion, regionSelectedRowType, regionFileColumns, {"r_name =
                    //       'AMERICA'"})
                    .tableScan(Table::TBL_REGION, std::move(regionColumns), scaleFactor)
                    .capturePlanNodeId(regionScanNodeId)
                    // .filter("r_name = 'AMERICA'")
                    .planNode();

  auto part = PlanBuilder(planNodeIdGenerator)
                  //   .tableScan(kPart,
                  //              partSelectedRowType,
                  //              partFileColumns,
                  //              {"p_type = 'ECONOMY ANODIZED STEEL'"})
                  .tableScan(Table::TBL_PART, std::move(partColumns), scaleFactor)
                  .capturePlanNodeId(partScanNodeId)
                  //   .filter("p_type = 'ECONOMY ANODIZED STEEL'")
                  .planNode();

  auto nationJoinRegion =
      PlanBuilder(planNodeIdGenerator)
          //   .tableScan(kNation, nationSelectedRowType, nationFileColumns)
          .tableScan(Table::TBL_NATION, std::move(nationColumns), scaleFactor)
          .capturePlanNodeId(nationScanNodeId)
          .hashJoin({"n_regionkey"}, {"r_regionkey"}, region, "", {"n_nationkey"})
          .planNode();

  auto customerJoinNationJoinRegion =
      PlanBuilder(planNodeIdGenerator)
          //   .tableScan(kCustomer, customerSelectedRowType, customerFileColumns)
          .tableScan(Table::TBL_CUSTOMER, std::move(customerColumns), scaleFactor)
          .capturePlanNodeId(customerScanNodeId)
          .hashJoin({"c_nationkey"}, {"n_nationkey"}, nationJoinRegion, "", {"c_custkey"})
          .planNode();

  auto ordersJoinCustomerJoinNationJoinRegion =
      PlanBuilder(planNodeIdGenerator)
          //   .tableScan(kOrders, ordersSelectedRowType, ordersFileColumns,
          //   {orderDateFilter})
          .tableScan(Table::TBL_ORDERS, std::move(ordersColumns), scaleFactor)
          .capturePlanNodeId(ordersScanNodeId)
          //   .filter(orderDateFilter)
          .hashJoin({"o_custkey"},
                    {"c_custkey"},
                    customerJoinNationJoinRegion,
                    "",
                    {"o_orderkey", "o_orderdate"})
          .planNode();

  auto supplierJoinNation =
      PlanBuilder(planNodeIdGenerator)
          //   .tableScan(kSupplier, supplierSelectedRowType, supplierFileColumns)
          .tableScan(Table::TBL_SUPPLIER, std::move(supplierColumns), scaleFactor)
          .capturePlanNodeId(supplierScanNodeId)
          .hashJoin({"s_nationkey"},
                    {"n_nationkey"},
                    nationWithName,
                    "",
                    {"s_suppkey", "n_name"})
          .planNode();

  auto plan =
      PlanBuilder(planNodeIdGenerator)
          //   .tableScan(kLineitem, lineitemSelectedRowType, lineitemFileColumns)
          .tableScan(Table::TBL_LINEITEM, std::move(lineitemColumns), scaleFactor)
          .capturePlanNodeId(lineitemScanNodeId)
          .hashJoin(
              {"l_orderkey"},
              {"o_orderkey"},
              ordersJoinCustomerJoinNationJoinRegion,
              "",
              {"l_partkey", "l_suppkey", "o_orderdate", "l_extendedprice", "l_discount"})
          .hashJoin(
              {"l_suppkey"},
              {"s_suppkey"},
              supplierJoinNation,
              "",
              {"n_name", "o_orderdate", "l_partkey", "l_extendedprice", "l_discount"})
          .hashJoin({"l_partkey"},
                    {"p_partkey"},
                    part,
                    "",
                    {"n_name", "o_orderdate", "l_extendedprice", "l_discount"})
          .project(
              {"l_extendedprice * (1.0 - l_discount) as volume", "n_name", "o_orderdate"})
          .project(
              {"volume",
               "(CASE WHEN n_name = 'BRAZIL' THEN volume ELSE 0.0 END) as brazil_volume",
               "year(o_orderdate) AS o_year"})
          .partialAggregation(
              {"o_year"},
              {"sum(brazil_volume) as volume_brazil", "sum(volume) as volume_all"})
          .localPartition({})
          .finalAggregation()
          .orderBy({"o_year"}, false)
          .project({"o_year", "(volume_brazil / volume_all) as mkt_share"})
          .planNode();

  TpchPlan context;
  context.plan = std::move(plan);
  context.scanNodes.emplace(partScanNodeId);
  context.scanNodes.emplace(supplierScanNodeId);
  context.scanNodes.emplace(lineitemScanNodeId);
  context.scanNodes.emplace(ordersScanNodeId);
  context.scanNodes.emplace(customerScanNodeId);
  context.scanNodes.emplace(nationScanNodeId);
  context.scanNodes.emplace(nationScanNodeIdWithName);
  context.scanNodes.emplace(regionScanNodeId);
  return context;
}

TpchPlan TpchInMemBuilder::getQ9Plan(double scaleFactor) const {
  std::vector<std::string> lineitemColumns = {"l_suppkey",
                                              "l_partkey",
                                              "l_discount",
                                              "l_extendedprice",
                                              "l_orderkey",
                                              "l_quantity"};
  std::vector<std::string> lineitemColumns1 = {"l_suppkey",
                                               "l_partkey",
                                               "l_discount",
                                               "l_extendedprice",
                                               "l_orderkey",
                                               "l_quantity"};
  std::vector<std::string> partColumns = {"p_name", "p_partkey"};
  std::vector<std::string> supplierColumns = {"s_suppkey", "s_nationkey"};
  std::vector<std::string> partsuppColumns = {
      "ps_partkey", "ps_suppkey", "ps_supplycost"};
  std::vector<std::string> ordersColumns = {"o_orderkey", "o_orderdate"};
  std::vector<std::string> nationColumns = {"n_nationkey", "n_name"};

  //   auto partSelectedRowType = getRowType(kPart, partColumns);
  //   const auto& partFileColumns = getFileColumnNames(kPart);
  //   auto supplierSelectedRowType = getRowType(kSupplier, supplierColumns);
  //   const auto& supplierFileColumns = getFileColumnNames(kSupplier);
  //   auto lineitemSelectedRowType = getRowType(kLineitem, lineitemColumns);
  //   const auto& lineitemFileColumns = getFileColumnNames(kLineitem);
  //   auto partsuppSelectedRowType = getRowType(kPartsupp, partsuppColumns);
  //   const auto& partsuppFileColumns = getFileColumnNames(kPartsupp);
  //   auto ordersSelectedRowType = getRowType(kOrders, ordersColumns);
  //   const auto& ordersFileColumns = getFileColumnNames(kOrders);
  //   auto nationSelectedRowType = getRowType(kNation, nationColumns);
  //   const auto& nationFileColumns = getFileColumnNames(kNation);

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId partScanNodeId;
  core::PlanNodeId supplierScanNodeId;
  core::PlanNodeId lineitemScanNodeId;
  core::PlanNodeId partsuppScanNodeId;
  core::PlanNodeId ordersScanNodeId;
  core::PlanNodeId nationScanNodeId;

  const std::vector<std::string> lineitemCommonColumns = {
      "l_extendedprice", "l_discount", "l_quantity"};

  auto part = PlanBuilder(planNodeIdGenerator)
                  //   .tableScan(
                  //       kPart, partSelectedRowType, partFileColumns, {}, "p_name like
                  //       '%green%'")
                  .tableScan(Table::TBL_PART, std::move(partColumns), scaleFactor)
                  .capturePlanNodeId(partScanNodeId)
                  //   .filter("p_name like '%green%'")
                  .planNode();

  auto supplier =
      PlanBuilder(planNodeIdGenerator)
          //   .tableScan(kSupplier, supplierSelectedRowType, supplierFileColumns)
          .tableScan(Table::TBL_SUPPLIER, std::move(supplierColumns), scaleFactor)
          .capturePlanNodeId(supplierScanNodeId)
          .planNode();

  auto nation = PlanBuilder(planNodeIdGenerator)
                    // .tableScan(kNation, nationSelectedRowType, nationFileColumns)
                    .tableScan(Table::TBL_NATION, std::move(nationColumns), scaleFactor)
                    .capturePlanNodeId(nationScanNodeId)
                    .planNode();

  auto lineitemJoinPartJoinSupplier =
      PlanBuilder(planNodeIdGenerator)
          //   .tableScan(kLineitem, lineitemSelectedRowType, lineitemFileColumns)
          .tableScan(Table::TBL_LINEITEM, std::move(lineitemColumns), scaleFactor)
          .capturePlanNodeId(lineitemScanNodeId)
          .hashJoin({"l_partkey"}, {"p_partkey"}, part, "", lineitemColumns)
          .hashJoin({"l_suppkey"},
                    {"s_suppkey"},
                    supplier,
                    "",
                    mergeColumnNames(lineitemColumns1, {"s_nationkey"}))
          .planNode();

  auto partsuppJoinLineitemJoinPartJoinSupplier =
      PlanBuilder(planNodeIdGenerator)
          //   .tableScan(kPartsupp, partsuppSelectedRowType, partsuppFileColumns)
          .tableScan(Table::TBL_PARTSUPP, std::move(partsuppColumns), scaleFactor)
          .capturePlanNodeId(partsuppScanNodeId)
          .hashJoin({"ps_partkey", "ps_suppkey"},
                    {"l_partkey", "l_suppkey"},
                    lineitemJoinPartJoinSupplier,
                    "",
                    mergeColumnNames(lineitemCommonColumns,
                                     {"l_orderkey", "s_nationkey", "ps_supplycost"}))
          .planNode();

  auto plan =
      PlanBuilder(planNodeIdGenerator)
          //   .tableScan(kOrders, ordersSelectedRowType, ordersFileColumns)
          .tableScan(Table::TBL_ORDERS, std::move(ordersColumns), scaleFactor)
          .capturePlanNodeId(ordersScanNodeId)
          .hashJoin({"o_orderkey"},
                    {"l_orderkey"},
                    partsuppJoinLineitemJoinPartJoinSupplier,
                    "",
                    mergeColumnNames(lineitemCommonColumns,
                                     {"s_nationkey", "ps_supplycost", "o_orderdate"}))
          .hashJoin({"s_nationkey"},
                    {"n_nationkey"},
                    nation,
                    "",
                    mergeColumnNames(lineitemCommonColumns,
                                     {"ps_supplycost", "o_orderdate", "n_name"}))
          .project({"n_name AS nation",
                    "year(o_orderdate) AS o_year",
                    "l_extendedprice * (1.0 - l_discount) - ps_supplycost * l_quantity "
                    "AS amount"})
          .partialAggregation({"nation", "o_year"}, {"sum(amount) AS sum_profit"})
          .localPartition({})
          .finalAggregation()
          .orderBy({"nation", "o_year DESC"}, false)
          .planNode();

  TpchPlan context;
  context.plan = std::move(plan);
  context.scanNodes.emplace(partScanNodeId);
  context.scanNodes.emplace(supplierScanNodeId);
  context.scanNodes.emplace(lineitemScanNodeId);
  context.scanNodes.emplace(partsuppScanNodeId);
  context.scanNodes.emplace(ordersScanNodeId);
  context.scanNodes.emplace(nationScanNodeId);
  return context;
}

TpchPlan TpchInMemBuilder::getQ10Plan(double scaleFactor) const {
  std::vector<std::string> customerColumns = {"c_nationkey",
                                              "c_custkey",
                                              "c_acctbal",
                                              "c_name",
                                              "c_address",
                                              "c_phone",
                                              "c_comment"};
  std::vector<std::string> nationColumns = {"n_nationkey", "n_name"};
  std::vector<std::string> lineitemColumns = {
      "l_orderkey", "l_returnflag", "l_extendedprice", "l_discount"};
  std::vector<std::string> ordersColumns = {"o_orderdate", "o_orderkey", "o_custkey"};

  //   const auto customerSelectedRowType = getRowType(kCustomer, customerColumns);
  //   const auto& customerFileColumns = getFileColumnNames(kCustomer);
  //   const auto nationSelectedRowType = getRowType(kNation, nationColumns);
  //   const auto& nationFileColumns = getFileColumnNames(kNation);
  //   const auto lineitemSelectedRowType = getRowType(kLineitem, lineitemColumns);
  //   const auto& lineitemFileColumns = getFileColumnNames(kLineitem);
  //   const auto ordersSelectedRowType = getRowType(kOrders, ordersColumns);
  //   const auto& ordersFileColumns = getFileColumnNames(kOrders);

  const auto lineitemReturnFlagFilter = "l_returnflag = 'R'";
  const auto orderDate = "o_orderdate";
  auto orderDateFilter = formatDateFilter(orderDate, "'1993-10-01'", "'1993-12-31'");

  const std::vector<std::string> customerOutputColumns = {
      "c_name", "c_acctbal", "c_phone", "c_address", "c_custkey", "c_comment"};

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId customerScanNodeId;
  core::PlanNodeId nationScanNodeId;
  core::PlanNodeId lineitemScanNodeId;
  core::PlanNodeId ordersScanNodeId;

  auto nation = PlanBuilder(planNodeIdGenerator)
                    // .tableScan(kNation, nationSelectedRowType, nationFileColumns)
                    .tableScan(Table::TBL_NATION, std::move(nationColumns), scaleFactor)
                    .capturePlanNodeId(nationScanNodeId)
                    .planNode();

  auto orders = PlanBuilder(planNodeIdGenerator)
                    //   .tableScan(kOrders, ordersSelectedRowType, ordersFileColumns,
                    //   {orderDateFilter})
                    .tableScan(Table::TBL_ORDERS, std::move(ordersColumns), scaleFactor)
                    .capturePlanNodeId(ordersScanNodeId)
                    // .filter(orderDateFilter)
                    .planNode();

  auto partialPlan =
      PlanBuilder(planNodeIdGenerator)
          //   .tableScan(kCustomer, customerSelectedRowType, customerFileColumns)
          .tableScan(Table::TBL_CUSTOMER, std::move(customerColumns), scaleFactor)
          .capturePlanNodeId(customerScanNodeId)
          .hashJoin(
              {"c_custkey"},
              {"o_custkey"},
              orders,
              "",
              mergeColumnNames(customerOutputColumns, {"c_nationkey", "o_orderkey"}))
          .hashJoin({"c_nationkey"},
                    {"n_nationkey"},
                    nation,
                    "",
                    mergeColumnNames(customerOutputColumns, {"n_name", "o_orderkey"}))
          .planNode();

  auto plan =
      PlanBuilder(planNodeIdGenerator)
          //   .tableScan(kLineitem,
          //              lineitemSelectedRowType,
          //              lineitemFileColumns,
          //              {lineitemReturnFlagFilter})
          .tableScan(Table::TBL_LINEITEM, std::move(lineitemColumns), scaleFactor)
          .capturePlanNodeId(lineitemScanNodeId)
          //   .filter(lineitemReturnFlagFilter)
          .project({"l_extendedprice * (1.0 - l_discount) AS part_revenue", "l_orderkey"})
          .hashJoin({"l_orderkey"},
                    {"o_orderkey"},
                    partialPlan,
                    "",
                    mergeColumnNames(customerOutputColumns, {"part_revenue", "n_name"}))
          .partialAggregation({"c_custkey",
                               "c_name",
                               "c_acctbal",
                               "n_name",
                               "c_address",
                               "c_phone",
                               "c_comment"},
                              {"sum(part_revenue) as revenue"})
          .localPartition({})
          .finalAggregation()
          .orderBy({"revenue DESC"}, false)
          .project({"c_custkey",
                    "c_name",
                    "revenue",
                    "c_acctbal",
                    "n_name",
                    "c_address",
                    "c_phone",
                    "c_comment"})
          .limit(0, 20, false)
          .planNode();

  TpchPlan context;
  context.plan = std::move(plan);
  context.scanNodes.emplace(customerScanNodeId);
  context.scanNodes.emplace(nationScanNodeId);
  context.scanNodes.emplace(lineitemScanNodeId);
  context.scanNodes.emplace(ordersScanNodeId);
  return context;
}

TpchPlan TpchInMemBuilder::getQ12Plan(double scaleFactor) const {
  std::vector<std::string> ordersColumns = {"o_orderkey", "o_orderpriority"};
  std::vector<std::string> lineitemColumns = {
      "l_receiptdate", "l_orderkey", "l_commitdate", "l_shipmode", "l_shipdate"};

  //   auto ordersSelectedRowType = getRowType(kOrders, ordersColumns);
  //   const auto& ordersFileColumns = getFileColumnNames(kOrders);
  //   auto lineitemSelectedRowType = getRowType(kLineitem, lineitemColumns);
  //   const auto& lineitemFileColumns = getFileColumnNames(kLineitem);

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId ordersScanNodeId;
  core::PlanNodeId lineitemScanNodeId;

  const std::string receiptDateFilter =
      formatDateFilter("l_receiptdate", "'1994-01-01'", "'1994-12-31'");
  const std::string shipDateFilter = formatDateFilter("l_shipdate", "", "'1995-01-01'");
  const std::string commitDateFilter =
      formatDateFilter("l_commitdate", "", "'1995-01-01'");

  auto lineitem =
      PlanBuilder(planNodeIdGenerator, pool_.get())
          //   .tableScan(kLineitem,
          //              lineitemSelectedRowType,
          //              lineitemFileColumns,
          //              {receiptDateFilter,
          //               "l_shipmode IN ('MAIL', 'SHIP')",
          //               shipDateFilter,
          //               commitDateFilter},
          //              "l_commitdate < l_receiptdate")
          .tableScan(Table::TBL_LINEITEM, std::move(lineitemColumns), scaleFactor)
          .capturePlanNodeId(lineitemScanNodeId)
          //   .filter(receiptDateFilter)
          //   .filter("l_shipmode IN ('MAIL', 'SHIP')")
          //   .filter(shipDateFilter)
          //   .filter(commitDateFilter)
          //   .filter("l_commitdate < l_receiptdate")
          //   .filter("l_shipdate < l_commitdate")
          .planNode();

  auto plan =
      PlanBuilder(planNodeIdGenerator)
          //   .tableScan(kOrders, ordersSelectedRowType, ordersFileColumns, {})
          .tableScan(Table::TBL_ORDERS, std::move(ordersColumns), scaleFactor)
          .capturePlanNodeId(ordersScanNodeId)
          .hashJoin({"o_orderkey"},
                    {"l_orderkey"},
                    lineitem,
                    "",
                    {"l_shipmode", "o_orderpriority"})
          .project({"l_shipmode",
                    "(CASE WHEN o_orderpriority = '1-URGENT' OR o_orderpriority = "
                    "'2-HIGH' THEN 1 ELSE 0 END) AS high_line_count_partial",
                    "(CASE WHEN o_orderpriority <> '1-URGENT' AND o_orderpriority <> "
                    "'2-HIGH' THEN 1 ELSE 0 END) AS low_line_count_partial"})
          .partialAggregation({"l_shipmode"},
                              {"sum(high_line_count_partial) as high_line_count",
                               "sum(low_line_count_partial) as low_line_count"})
          .localPartition({})
          .finalAggregation()
          .orderBy({"l_shipmode"}, false)
          .planNode();

  TpchPlan context;
  context.plan = std::move(plan);
  context.scanNodes.emplace(ordersScanNodeId);
  context.scanNodes.emplace(lineitemScanNodeId);
  return context;
}

TpchPlan TpchInMemBuilder::getQ13Plan(double scaleFactor) const {
  std::vector<std::string> ordersColumns = {"o_custkey", "o_comment", "o_orderkey"};
  std::vector<std::string> customerColumns = {"c_custkey"};

  //   const auto ordersSelectedRowType = getRowType(kOrders, ordersColumns);
  //   const auto& ordersFileColumns = getFileColumnNames(kOrders);

  //   const auto customerSelectedRowType = getRowType(kCustomer, customerColumns);
  //   const auto& customerFileColumns = getFileColumnNames(kCustomer);

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId customerScanNodeId;
  core::PlanNodeId ordersScanNodeId;

  auto customers =
      PlanBuilder(planNodeIdGenerator)
          //    .tableScan(kCustomer, customerSelectedRowType, customerFileColumns)
          .tableScan(Table::TBL_CUSTOMER, std::move(customerColumns), scaleFactor)
          .capturePlanNodeId(customerScanNodeId)
          .planNode();

  auto plan =
      PlanBuilder(planNodeIdGenerator)
          //   .tableScan(kOrders,
          //              ordersSelectedRowType,
          //              ordersFileColumns,
          //              {},
          //              "o_comment not like '%special%requests%'")
          .tableScan(Table::TBL_ORDERS, std::move(ordersColumns), scaleFactor)
          .capturePlanNodeId(ordersScanNodeId)
          //   .filter("o_comment not like '%special%requests%'")
          .hashJoin({"o_custkey"},
                    {"c_custkey"},
                    customers,
                    "",
                    {"c_custkey", "o_orderkey"},
                    core::JoinType::kRight)
          .partialAggregation({"c_custkey"}, {"count(o_orderkey) as pc_count"})
          .localPartition({})
          .finalAggregation({"c_custkey"}, {"count(pc_count) as c_count"}, {BIGINT()})
          .singleAggregation({"c_count"}, {"count(0) as custdist"})
          .orderBy({"custdist DESC", "c_count DESC"}, false)
          .planNode();

  TpchPlan context;
  context.plan = std::move(plan);
  context.scanNodes.emplace(ordersScanNodeId);
  context.scanNodes.emplace(customerScanNodeId);
  return context;
}

TpchPlan TpchInMemBuilder::getQ14Plan(double scaleFactor) const {
  std::vector<std::string> lineitemColumns = {
      "l_partkey", "l_extendedprice", "l_discount", "l_shipdate"};
  std::vector<std::string> partColumns = {"p_partkey", "p_type"};

  //   auto lineitemSelectedRowType = getRowType(kLineitem, lineitemColumns);
  //   const auto& lineitemFileColumns = getFileColumnNames(kLineitem);
  //   auto partSelectedRowType = getRowType(kPart, partColumns);
  //   const auto& partFileColumns = getFileColumnNames(kPart);

  const std::string shipDate = "l_shipdate";
  const std::string shipDateFilter =
      formatDateFilter(shipDate, "'1995-09-01'", "'1995-09-30'");

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId lineitemScanNodeId;
  core::PlanNodeId partScanNodeId;

  auto part = PlanBuilder(planNodeIdGenerator)
                  //   .tableScan(kPart, partSelectedRowType, partFileColumns)
                  .tableScan(Table::TBL_PART, std::move(partColumns), scaleFactor)
                  .capturePlanNodeId(partScanNodeId)
                  .planNode();

  auto plan =
      PlanBuilder(planNodeIdGenerator)
          //   .tableScan(
          //       kLineitem, lineitemSelectedRowType, lineitemFileColumns, {},
          //       shipDateFilter)
          .tableScan(Table::TBL_LINEITEM, std::move(lineitemColumns), scaleFactor)
          .capturePlanNodeId(lineitemScanNodeId)
          //   .filter(shipDateFilter)
          .project({"l_extendedprice * (1.0 - l_discount) as part_revenue",
                    "l_shipdate",
                    "l_partkey"})
          .hashJoin({"l_partkey"}, {"p_partkey"}, part, "", {"part_revenue", "p_type"})
          .project({"(CASE WHEN (p_type LIKE 'PROMO%') THEN part_revenue ELSE 0.0 END) "
                    "as filter_revenue",
                    "part_revenue"})
          .partialAggregation({},
                              {"sum(part_revenue) as total_revenue",
                               "sum(filter_revenue) as total_promo_revenue"})
          .localPartition({})
          .finalAggregation()
          .project({"100.00 * total_promo_revenue/total_revenue as promo_revenue"})
          .planNode();

  TpchPlan context;
  context.plan = std::move(plan);
  context.scanNodes.emplace(lineitemScanNodeId);
  context.scanNodes.emplace(partScanNodeId);
  return context;
}

TpchPlan TpchInMemBuilder::getQ15Plan(double scaleFactor) const {
  std::vector<std::string> lineitemColumns = {
      "l_suppkey", "l_shipdate", "l_extendedprice", "l_discount"};
  std::vector<std::string> lineitemColumns2 = {
      "l_suppkey", "l_shipdate", "l_extendedprice", "l_discount"};
  std::vector<std::string> supplierColumns = {
      "s_suppkey", "s_name", "s_address", "s_phone"};

  //   const auto lineitemSelectedRowType = getRowType(kLineitem, lineitemColumns);
  //   const auto& lineitemFileColumns = getFileColumnNames(kLineitem);
  //   const auto supplierSelectedRowType = getRowType(kSupplier, supplierColumns);
  //   const auto& supplierFileColumns = getFileColumnNames(kSupplier);

  const std::string shipDateFilter =
      formatDateFilter("l_shipdate", "'1996-01-01'", "'1996-03-31'");

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId lineitemScanNodeIdSubQuery;
  core::PlanNodeId lineitemScanNodeId;
  core::PlanNodeId supplierScanNodeId;

  auto maxRevenue =
      PlanBuilder(planNodeIdGenerator)
          //   .tableScan(
          //       kLineitem, lineitemSelectedRowType, lineitemFileColumns,
          //       {shipDateFilter})
          .tableScan(Table::TBL_LINEITEM, std::move(lineitemColumns), scaleFactor)
          .capturePlanNodeId(lineitemScanNodeId)
          //   .filter(shipDateFilter)
          .project({"l_suppkey", "l_extendedprice * (1.0 - l_discount) as part_revenue"})
          .partialAggregation({"l_suppkey"}, {"sum(part_revenue) as total_revenue"})
          .localPartition({})
          .finalAggregation()
          .singleAggregation({}, {"max(total_revenue) as max_revenue"})
          .planNode();

  auto supplierWithMaxRevenue =
      PlanBuilder(planNodeIdGenerator)
          //   .tableScan(
          //       kLineitem, lineitemSelectedRowType, lineitemFileColumns,
          //       {shipDateFilter})
          .tableScan(Table::TBL_LINEITEM, std::move(lineitemColumns2), scaleFactor)
          .capturePlanNodeId(lineitemScanNodeIdSubQuery)
          //   .filter(shipDateFilter)
          .project({"l_suppkey as supplier_no",
                    "l_extendedprice * (1.0 - l_discount) as part_revenue"})
          .partialAggregation({"supplier_no"}, {"sum(part_revenue) as total_revenue"})
          .localPartition({})
          .finalAggregation()
          .hashJoin({"total_revenue"},
                    {"max_revenue"},
                    maxRevenue,
                    "",
                    {"supplier_no", "total_revenue"})
          .planNode();

  auto plan =
      PlanBuilder(planNodeIdGenerator)
          //   .tableScan(kSupplier, supplierSelectedRowType, supplierFileColumns)
          .tableScan(Table::TBL_SUPPLIER, std::move(supplierColumns), scaleFactor)
          .capturePlanNodeId(supplierScanNodeId)
          .hashJoin({"s_suppkey"},
                    {"supplier_no"},
                    supplierWithMaxRevenue,
                    "",
                    {"s_suppkey", "s_name", "s_address", "s_phone", "total_revenue"})
          .orderBy({"s_suppkey"}, false)
          .planNode();

  TpchPlan context;
  context.plan = std::move(plan);
  context.scanNodes.emplace(lineitemScanNodeIdSubQuery);
  context.scanNodes.emplace(lineitemScanNodeId);
  context.scanNodes.emplace(supplierScanNodeId);
  return context;
}

TpchPlan TpchInMemBuilder::getQ16Plan(double scaleFactor) const {
  std::vector<std::string> partColumns = {"p_brand", "p_type", "p_size", "p_partkey"};
  std::vector<std::string> supplierColumns = {"s_suppkey", "s_comment"};
  std::vector<std::string> partsuppColumns = {"ps_partkey", "ps_suppkey"};

  //   const auto partSelectedRowType = getRowType(kPart, partColumns);
  //   const auto& partFileColumns = getFileColumnNames(kPart);
  //   const auto supplierSelectedRowType = getRowType(kSupplier, supplierColumns);
  //   const auto& supplierFileColumns = getFileColumnNames(kSupplier);
  //   const auto partsuppSelectedRowType = getRowType(kPartsupp, partsuppColumns);
  //   const auto& partsuppFileColumns = getFileColumnNames(kPartsupp);

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId partScanNodeId;
  core::PlanNodeId supplierScanNodeId;
  core::PlanNodeId partsuppScanNodeId;

  auto part = PlanBuilder(planNodeIdGenerator, pool_.get())
                  //   .tableScan(kPart,
                  //              partSelectedRowType,
                  //              partFileColumns,
                  //              {"p_size in (49, 14, 23, 45, 19, 3, 36, 9)"},
                  //              "p_type NOT LIKE 'MEDIUM POLISHED%'")
                  .tableScan(Table::TBL_PART, std::move(partsuppColumns), scaleFactor)
                  .capturePlanNodeId(partScanNodeId)
                  // Neq is unsupported as a tableScan subfield filter for
                  // Parquet source.
                  //   .filter("p_size in (49, 14, 23, 45, 19, 3, 36, 9)")
                  //   .filter("p_type NOT LIKE 'MEDIUM POLISHED%'")
                  //   .filter("p_brand <> 'Brand#45'")
                  .planNode();

  auto supplier =
      PlanBuilder(planNodeIdGenerator)
          //   .tableScan(kSupplier,
          //              supplierSelectedRowType,
          //              supplierFileColumns,
          //              {},
          //              "s_comment LIKE '%Customer%Complaints%'")
          .tableScan(Table::TBL_SUPPLIER, std::move(supplierColumns), scaleFactor)
          .capturePlanNodeId(supplierScanNodeId)
          //   .filter("s_comment LIKE '%Customer%Complaints%'")
          .planNode();

  auto plan = PlanBuilder(planNodeIdGenerator)
                  //   .tableScan(kPartsupp, partsuppSelectedRowType, partsuppFileColumns)
                  .tableScan(Table::TBL_PARTSUPP, std::move(partsuppColumns), scaleFactor)
                  .capturePlanNodeId(partsuppScanNodeId)
                  .hashJoin({"ps_partkey"},
                            {"p_partkey"},
                            part,
                            "",
                            {"ps_suppkey", "p_brand", "p_type", "p_size"})
                  .hashJoin({"ps_suppkey"},
                            {"s_suppkey"},
                            supplier,
                            "",
                            {"ps_suppkey", "p_brand", "p_type", "p_size"},
                            core::JoinType::kNullAwareAnti)
                  // Empty aggregate is used here to get the distinct count of
                  // ps_suppkey.
                  // approx_distinct could be used instead for getting the count of
                  // distinct ps_suppkey but since approx_distinct is non deterministic
                  // and the standard error can not be set to 0, it is not used here.
                  .partialAggregation({"p_brand", "p_type", "p_size", "ps_suppkey"}, {})
                  .localPartition({"p_brand", "p_type", "p_size", "ps_suppkey"})
                  .finalAggregation()
                  .partialAggregation({"p_brand", "p_type", "p_size"},
                                      {"count(ps_suppkey) as supplier_cnt"})
                  .localPartition({})
                  .finalAggregation()
                  .orderBy({"supplier_cnt DESC", "p_brand", "p_type", "p_size"}, false)
                  .planNode();

  TpchPlan context;
  context.plan = std::move(plan);
  context.scanNodes.emplace(partScanNodeId);
  context.scanNodes.emplace(supplierScanNodeId);
  context.scanNodes.emplace(partsuppScanNodeId);
  return context;
}

TpchPlan TpchInMemBuilder::getQ18Plan(double scaleFactor) const {
  std::vector<std::string> lineitemColumns = {"l_orderkey", "l_quantity"};
  std::vector<std::string> ordersColumns = {
      "o_orderkey", "o_custkey", "o_orderdate", "o_totalprice"};
  std::vector<std::string> customerColumns = {"c_name", "c_custkey"};

  //   const auto lineitemSelectedRowType = getRowType(kLineitem, lineitemColumns);
  //   const auto& lineitemFileColumns = getFileColumnNames(kLineitem);

  //   const auto ordersSelectedRowType = getRowType(kOrders, ordersColumns);
  //   const auto& ordersFileColumns = getFileColumnNames(kOrders);

  //   const auto customerSelectedRowType = getRowType(kCustomer, customerColumns);
  //   const auto& customerFileColumns = getFileColumnNames(kCustomer);

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId customerScanNodeId;
  core::PlanNodeId ordersScanNodeId;
  core::PlanNodeId lineitemScanNodeId;

  auto bigOrders =
      PlanBuilder(planNodeIdGenerator)
          //   .tableScan(kLineitem, lineitemSelectedRowType, lineitemFileColumns)
          .tableScan(Table::TBL_LINEITEM, std::move(lineitemColumns), scaleFactor)
          .capturePlanNodeId(lineitemScanNodeId)
          .partialAggregation({"l_orderkey"}, {"sum(l_quantity) AS partial_sum"})
          .localPartition({"l_orderkey"})
          .finalAggregation({"l_orderkey"}, {"sum(partial_sum) AS quantity"}, {DOUBLE()})
          //   .filter("quantity > 300.0")
          .planNode();

  auto plan =
      PlanBuilder(planNodeIdGenerator)
          //   .tableScan(kOrders, ordersSelectedRowType, ordersFileColumns)
          .tableScan(Table::TBL_ORDERS, std::move(ordersColumns), scaleFactor)
          .capturePlanNodeId(ordersScanNodeId)
          .hashJoin({"o_orderkey"},
                    {"l_orderkey"},
                    bigOrders,
                    "",
                    {"o_orderkey",
                     "o_custkey",
                     "o_orderdate",
                     "o_totalprice",
                     "l_orderkey",
                     "quantity"})
          .hashJoin(
              {"o_custkey"},
              {"c_custkey"},
              PlanBuilder(planNodeIdGenerator)
                  //   .tableScan(kCustomer, customerSelectedRowType, customerFileColumns)
                  .tableScan(Table::TBL_CUSTOMER, std::move(customerColumns), scaleFactor)
                  .capturePlanNodeId(customerScanNodeId)
                  .planNode(),
              "",
              {"c_name",
               "c_custkey",
               "o_orderkey",
               "o_orderdate",
               "o_totalprice",
               "quantity"})
          .localPartition({})
          .orderBy({"o_totalprice DESC", "o_orderdate"}, false)
          .limit(0, 100, false)
          .planNode();

  TpchPlan context;
  context.plan = std::move(plan);
  context.scanNodes.emplace(lineitemScanNodeId);
  context.scanNodes.emplace(ordersScanNodeId);
  context.scanNodes.emplace(customerScanNodeId);
  return context;
}

TpchPlan TpchInMemBuilder::getQ19Plan(double scaleFactor) const {
  std::vector<std::string> lineitemColumns = {"l_partkey",
                                              "l_shipmode",
                                              "l_shipinstruct",
                                              "l_extendedprice",
                                              "l_discount",
                                              "l_quantity"};
  std::vector<std::string> partColumns = {
      "p_partkey", "p_brand", "p_container", "p_size"};

  //   auto lineitemSelectedRowType = getRowType(kLineitem, lineitemColumns);
  //   const auto& lineitemFileColumns = getFileColumnNames(kLineitem);
  //   auto partSelectedRowType = getRowType(kPart, partColumns);
  //   const auto& partFileColumns = getFileColumnNames(kPart);

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId lineitemScanNodeId;
  core::PlanNodeId partScanNodeId;

  const std::string shipModeFilter = "l_shipmode IN ('AIR', 'AIR REG')";
  const std::string shipInstructFilter = "(l_shipinstruct = 'DELIVER IN PERSON')";
  const std::string joinFilterExpr =
      "     ((p_brand = 'Brand#12')"
      "     AND (l_quantity between 1.0 and 11.0)"
      "     AND (p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG'))"
      "     AND (p_size BETWEEN 1 AND 5))"
      " OR  ((p_brand ='Brand#23')"
      "     AND (p_container IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK'))"
      "     AND (l_quantity between 10.0 and 20.0)"
      "     AND (p_size BETWEEN 1 AND 10))"
      " OR  ((p_brand = 'Brand#34')"
      "     AND (p_container IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG'))"
      "     AND (l_quantity between 20.0 and 30.0)"
      "     AND (p_size BETWEEN 1 AND 15))";

  auto part = PlanBuilder(planNodeIdGenerator)
                  //   .tableScan(kPart, partSelectedRowType, partFileColumns)
                  .tableScan(Table::TBL_PART, std::move(partColumns), scaleFactor)
                  .capturePlanNodeId(partScanNodeId)
                  .planNode();

  auto plan =
      PlanBuilder(planNodeIdGenerator, pool_.get())
          //   .tableScan(kLineitem,
          //              lineitemSelectedRowType,
          //              lineitemFileColumns,
          //              {shipModeFilter, shipInstructFilter})
          .tableScan(Table::TBL_LINEITEM, std::move(lineitemColumns), scaleFactor)
          .capturePlanNodeId(lineitemScanNodeId)
          //   .filter(shipModeFilter)
          //   .filter(shipInstructFilter)
          .project({"l_extendedprice * (1.0 - l_discount) as part_revenue",
                    "l_shipmode",
                    "l_shipinstruct",
                    "l_partkey",
                    "l_quantity"})
          .hashJoin({"l_partkey"}, {"p_partkey"}, part, joinFilterExpr, {"part_revenue"})
          .partialAggregation({}, {"sum(part_revenue) as revenue"})
          .localPartition({})
          .finalAggregation()
          .planNode();

  TpchPlan context;
  context.plan = std::move(plan);
  context.scanNodes.emplace(lineitemScanNodeId);
  context.scanNodes.emplace(partScanNodeId);
  return context;
}

TpchPlan TpchInMemBuilder::getQ22Plan(double scaleFactor) const {
  std::vector<std::string> ordersColumns = {"o_custkey"};
  std::vector<std::string> customerColumns = {"c_acctbal", "c_phone"};
  std::vector<std::string> customerColumnsWithKey = {"c_custkey", "c_acctbal", "c_phone"};

  //   const auto ordersSelectedRowType = getRowType(kOrders, ordersColumns);
  //   const auto& ordersFileColumns = getFileColumnNames(kOrders);
  //   const auto customerSelectedRowType = getRowType(kCustomer, customerColumns);
  //   const auto& customerFileColumns = getFileColumnNames(kCustomer);
  //   const auto customerSelectedRowTypeWithKey =
  //       getRowType(kCustomer, customerColumnsWithKey);
  //   const auto& customerFileColumnsWithKey = getFileColumnNames(kCustomer);

  const std::string phoneFilter =
      "substr(c_phone, 1, 2) IN ('13', '31', '23', '29', '30', '18', '17')";

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId customerScanNodeId;
  core::PlanNodeId customerScanNodeIdWithKey;
  core::PlanNodeId ordersScanNodeId;

  auto orders = PlanBuilder(planNodeIdGenerator)
                    // .tableScan(kOrders, ordersSelectedRowType, ordersFileColumns)
                    .tableScan(Table::TBL_ORDERS, std::move(ordersColumns), scaleFactor)
                    .capturePlanNodeId(ordersScanNodeId)
                    .planNode();

  auto customerAvgAccountBalance =
      PlanBuilder(planNodeIdGenerator, pool_.get())
          //   .tableScan(kCustomer,
          //              customerSelectedRowType,
          //              customerFileColumns,
          //              {"c_acctbal > 0.0"},
          //              phoneFilter)
          .tableScan(Table::TBL_CUSTOMER, std::move(customerColumns), scaleFactor)
          .capturePlanNodeId(customerScanNodeId)
          //   .filter("c_acctbal > 0.0")
          //   .filter(phoneFilter)
          .partialAggregation({}, {"avg(c_acctbal) as avg_acctbal"})
          .localPartition({})
          .finalAggregation()
          .planNode();

  auto plan =
      PlanBuilder(planNodeIdGenerator, pool_.get())
          //   .tableScan(kCustomer,
          //              customerSelectedRowTypeWithKey,
          //              customerFileColumnsWithKey,
          //              {},
          //              phoneFilter)
          .tableScan(Table::TBL_CUSTOMER, std::move(customerColumnsWithKey), scaleFactor)
          .capturePlanNodeId(customerScanNodeIdWithKey)
          //   .filter(phoneFilter)
          .crossJoin(customerAvgAccountBalance,
                     {"c_acctbal", "avg_acctbal", "c_custkey", "c_phone"})
          //   .filter("c_acctbal > avg_acctbal")
          .hashJoin({"c_custkey"},
                    {"o_custkey"},
                    orders,
                    "",
                    {"c_acctbal", "c_phone"},
                    core::JoinType::kNullAwareAnti)
          .project({"substr(c_phone, 1, 2) AS country_code", "c_acctbal"})
          .partialAggregation({"country_code"},
                              {"count(0) AS numcust", "sum(c_acctbal) AS totacctbal"})
          .localPartition({})
          .finalAggregation()
          .orderBy({"country_code"}, false)
          .planNode();

  TpchPlan context;
  context.plan = std::move(plan);
  context.scanNodes.emplace(ordersScanNodeId);
  context.scanNodes.emplace(customerScanNodeId);
  context.scanNodes.emplace(customerScanNodeIdWithKey);
  return context;
}

TpchPlan TpchInMemBuilder::getQ23Plan(double scaleFactor) const {
  std::vector<std::string> customerColumns = {"c_nationkey"};
  std::vector<std::string> nationColumns = {"n_nationkey"};
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId nationScanNodeId;
  core::PlanNodeId customerScanNodeId;

  auto customer =
      PlanBuilder(planNodeIdGenerator)
          .tableScan(Table::TBL_CUSTOMER, std::move(customerColumns), scaleFactor)
          .capturePlanNodeId(customerScanNodeId)
          .planNode();

  auto plan = PlanBuilder(planNodeIdGenerator)
                  .tableScan(Table::TBL_NATION, std::move(nationColumns), scaleFactor)
                  .capturePlanNodeId(nationScanNodeId)
                  //   .project({"n_nationkey"})
                  .hashJoin({"n_nationkey"}, {"c_nationkey"}, customer, "", {})
                  .localPartition({})
                  .planNode();

  TpchPlan context;
  context.plan = std::move(plan);
  context.scanNodes.emplace(nationScanNodeId);
  context.scanNodes.emplace(customerScanNodeId);
  return context;
}

}  // namespace facebook::velox::exec::test
