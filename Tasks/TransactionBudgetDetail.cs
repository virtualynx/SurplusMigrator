using Serilog;
using SurplusMigrator.Libraries;
using SurplusMigrator.Models;
using System;
using System.Collections.Generic;
using System.Data.Common;
using Microsoft.Data.SqlClient;
using System.Linq;
using System.Text.Json;

using BudgetId = System.Int64;
using BudgetAccountId = System.String;
using BudgetDetailId = System.Int64;
using JournalIdLine = System.String;

namespace SurplusMigrator.Tasks {
    class TransactionBudgetDetail : _BaseTask {
        /**
         * here holds ids of the missing TransactionBudget data
         * these TransactionBudget data also does not referred anywhere on TransactionJournal
         * so we can safely assumed that theyre safe to be ignored
         */
        private static List<long> allMissingBudgetIds = new List<long>() {
            //3398,
            //3467,
            //3829,
            //3852,
            //3850,
            //4021,
        };

        /**
         * Total count of ignored data caused by missing TransactionBudget
         */
        private static long totalMissingBudgetData = 0;

        /**
         * here holds ids of the TransactionBudgetDetail data which has null reference to budget_id and also has null creation-date
         * these TransactionBudgetDetail data also does not referred anywhere on TransactionJournalDetail
         * so we can safely assumed that theyre safe to be ignored
         */
        private static List<long> allUnreferencedDataIds = new List<long>() {};

        /**
         * Total count of ignored data caused by having null reference to budget_id and also null creation-date
         */
        private static long totalUnreferencedData = 0;

        public TransactionBudgetDetail(DbConnection_[] connections) : base(connections) {
            sources = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().dbname == "E_FRM").FirstOrDefault(),
                    tableName = "transaksi_budgetdetil",
                    columns = new string[] {
                        "budgetdetil_id",
                        "budgetdetil_line",
                        "budget_id",
                        "budgetdetil_date",
                        "acc_id",
                        "projectacc_id",
                        "budgetdetil_desc",
                        "budgetdetil_amount",
                        "currency_id",
                        //"budgetdetil_valas",
                        //"budgetdetil_comp",
                        "budgetdetil_rate",
                        "budgetdetil_eps",
                        "budgetdetil_unit",
                        "budgetdetil_days",
                        "budgetdetil_amountprop",
                        //"budgetdetil_valasprop",
                        //"budgetdetil_amountrev",
                        //"budgetdetil_valasrev",
                        //"budgetdetil_amountpaid",
                        //"budgetdetil_valaspaid",
                        //"budgetdetil_amountreq",
                        //"budgetdetil_valasreq",
                        "acc_id_bymhd",
                    },
                    ids = new string[] { "budgetdetil_id", "budgetdetil_line" }
                }
            };
            destinations = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().dbname == "insosys").FirstOrDefault(),
                    tableName = "transaction_budget_detail",
                    columns = new string[] {
                        "tbudget_detailid",
                        "descr",
                        "amount",
                        "rate",
                        "eps",
                        "unit",
                        "days",
                        "prop_amount",
                        "tbudgetid",
                        "accountid",
                        "budgetaccountid",
                        "currencyid",
                        "account_bymhd_id",
                        "isaccrued",
                        //"accrueddate",
                        //"accruedby",
                        //"accruedamount",
                        "created_date",
                        "created_by",
                        "is_disabled",
                        //"account_debt_id",
                    },
                    ids = new string[] { "tbudget_detailid" }
                }
            };
        }

        public override List<RowData<ColumnName, Data>> getSourceData(Table[] sourceTables, int batchSize = defaultBatchSize) {
            return sourceTables.Where(a => a.tableName == "transaksi_budgetdetil").FirstOrDefault().getDatas(batchSize);
        }

        public override MappedData mapData(List<RowData<ColumnName, Data>> inputs) {
            MappedData result = new MappedData();

            ///*
            /// there is some changes of ids on MasterBudgetAccount, so we also need-
            /// to change all reference to the respective changed ids.
            /// see Task MasterBudgetAccount for more detailed mapping
            Dictionary<BudgetId, BudgetAccountId> remappedBudgetAccounts = new Dictionary<BudgetId, BudgetAccountId>() {
                { 666026, "5040602201" },
                { 709454, "5040601001" },
            };

            List<long> missingBudgetIds = getMissingTransactionBudgetIds(inputs);
            if(missingBudgetIds.Count > 0) {
                allMissingBudgetIds.AddRange(missingBudgetIds);
                //List<RowData<ColumnName, Data>> ignoredDatas = inputs.Where(row => missingBudgetIds.Any(missingId => row.Any(map => map.Key == "budget_id" && Utils.obj2long(map.Value) == missingId))).ToList();
                long beforeFilteredCount = inputs.Count;
                inputs = inputs.Where(row => !missingBudgetIds.Any(missingId => row.Any(map => map.Key == "budget_id" && Utils.obj2long(map.Value) == missingId))).ToList();
                //totalMissingBudgetData += ignoredDatas.Count;
                totalMissingBudgetData += (beforeFilteredCount - inputs.Count);
            }
            if(allMissingBudgetIds.Count > 0) {
                Log.Logger.Warning("Total count of ignored data caused by \"missing data in table [transaksi_budget]\": " + totalMissingBudgetData);
            }
            
            List<long> unreferencedDataIds = getUnreferencedDataIds(inputs);
            if(unreferencedDataIds.Count > 0) {
                allUnreferencedDataIds.AddRange(unreferencedDataIds);
                inputs = inputs.Where(row => !unreferencedDataIds.Any(unreferencedId => row.Any(map => map.Key == "budgetdetil_id" && Utils.obj2long(map.Value) == unreferencedId))).ToList();
                totalUnreferencedData += unreferencedDataIds.Count;
            }
            if(allUnreferencedDataIds.Count > 0) {
                Log.Logger.Warning("Total count of ignored data caused by \"having null column [budget_id] & [budgetdetil_date]\": " + totalUnreferencedData);
            }

            List<long> missingCreationDateIds = fillsMissingCreationDate(inputs);

            foreach(RowData<ColumnName, Data> data in inputs) {
                string tbudgetid = null;
                if(Utils.obj2long(data["budget_id"]) > 0) {
                    tbudgetid = IdRemapper.get("tbudgetid", data["budget_id"]).ToString();
                }

                string budgetaccountid = Utils.obj2long(data["projectacc_id"])!=0? data["projectacc_id"].ToString(): null;
                long budgetdetil_id = Utils.obj2long(data["budgetdetil_id"]);
                if(remappedBudgetAccounts.ContainsKey(budgetdetil_id)) {
                    budgetaccountid = remappedBudgetAccounts[budgetdetil_id];
                }

                DateTime created_date = Utils.obj2datetime(data["budgetdetil_date"]);
                string tbudget_detailid = Sequencer.getId("BGTD", (DateTime)created_date);
                IdRemapper.add("tbudget_detailid", budgetdetil_id, tbudget_detailid);

                result.addData(
                    "transaction_budget_detail",
                    new RowData<ColumnName, Data>() {
                        { "tbudget_detailid",  tbudget_detailid},
                        { "descr",  data["budgetdetil_desc"]},
                        { "amount",  data["budgetdetil_amount"]==null? 0: data["budgetdetil_amount"]},
                        { "rate",  data["budgetdetil_rate"]==null? 0: data["budgetdetil_rate"]},
                        { "eps",  data["budgetdetil_eps"]==null? 0: data["budgetdetil_eps"]},
                        { "unit",  data["budgetdetil_unit"]==null? 0: data["budgetdetil_unit"]},
                        { "days",  data["budgetdetil_days"]==null? 0: data["budgetdetil_days"]},
                        { "prop_amount",  data["budgetdetil_amountprop"]==null? 0: data["budgetdetil_amountprop"]},
                        { "tbudgetid",  tbudgetid},
                        { "accountid",  Utils.obj2long(data["acc_id"])==0? null: data["acc_id"]},
                        { "budgetaccountid",  budgetaccountid},
                        { "currencyid",  data["currency_id"]==null? 0: data["currency_id"]},
                        { "account_bymhd_id",  Utils.obj2long(data["acc_id_bymhd"])==0? null: data["acc_id_bymhd"]},
                        { "isaccrued",  false},
                        { "created_date",  created_date},
                        { "created_by",  DefaultValues.CREATED_BY},
                        { "is_disabled", false},
                    }
                );
            }

            return result;
        }

        public override MappedData additionalStaticData() {
            return null;
        }

        public override void runDependencies() {
            new MasterBudgetAccount(connections).run();
        }

        private List<long> getUnreferencedDataIds(List<RowData<ColumnName, Data>> inputs) {
            List<long> nullBudgetAndDateIds = new List<long>();

            foreach(RowData<ColumnName, Data> row in inputs) {
                long budget_id = Utils.obj2long(row["budget_id"]);
                DateTime? budgetdetil_date = Utils.obj2datetimeNullable(row["budgetdetil_date"]);
                if(budget_id==0 && budgetdetil_date == null) {
                    nullBudgetAndDateIds.Add(Utils.obj2long(row["budgetdetil_id"]));
                }
            }

            if(nullBudgetAndDateIds.Count > 0) {
                SqlConnection conn = (SqlConnection)connections.Where(a => a.GetDbLoginInfo().dbname == "E_FRM").FirstOrDefault().GetDbConnection();
                SqlCommand command = new SqlCommand("select jurnal_id, jurnaldetil_line, budgetdetil_id from [dbo].[transaksi_jurnaldetil] where budgetdetil_id in (" + String.Join(",", nullBudgetAndDateIds) + ")", conn);
                SqlDataReader dataReader = command.ExecuteReader();

                Dictionary<BudgetDetailId, List<JournalIdLine>> referencedDatas = new Dictionary<BudgetDetailId, List<JournalIdLine>>();
                while(dataReader.Read()) {
                    long budgetdetil_id = Utils.obj2long(dataReader.GetValue(dataReader.GetOrdinal("budgetdetil_id")));
                    if(!referencedDatas.ContainsKey(budgetdetil_id)) {
                        referencedDatas[budgetdetil_id] = new List<JournalIdLine>();
                    }

                    string jurnal_id = Utils.obj2str(dataReader.GetValue(dataReader.GetOrdinal("jurnal_id")));
                    long jurnaldetil_line = Utils.obj2long(dataReader.GetValue(dataReader.GetOrdinal("jurnaldetil_line")));
                    string jurnalIdLineTag = jurnal_id + ";" + jurnaldetil_line;
                    referencedDatas[budgetdetil_id].Add(jurnalIdLineTag);
                }
                dataReader.Close();
                command.Dispose();

                if(referencedDatas.Count > 0) {
                    throw new Exception("Some of transaksi_budgetdetil has budget_id=null and budgetdetil_date=null, but being referenced in transaksi_jurnaldetil([budgetdetil_id -> list of jurnal_id;jurnaldetil_line] map): " + JsonSerializer.Serialize(referencedDatas));
                }

                //Log.Logger.Warning("transaction_budget_detail(ids) which has null budget_id & budgetdetil_date: " + JsonSerializer.Serialize(nullBudgetAndDateIds));
            }

            return nullBudgetAndDateIds;
        }

        private List<long> getMissingTransactionBudgetIds(List<RowData<ColumnName, Data>> inputs) {
            List<long> budgetIdsOfInputs = new List<long>();
            foreach(RowData<ColumnName, Data> row in inputs) {
                long budget_id = Utils.obj2long(row["budget_id"]);
                if(budget_id == 0) continue;
                if(!budgetIdsOfInputs.Contains(budget_id)) {
                    budgetIdsOfInputs.Add(budget_id);
                }
            }

            SqlConnection conn = (SqlConnection)connections.Where(a => a.GetDbLoginInfo().dbname == "E_FRM").FirstOrDefault().GetDbConnection();
            SqlCommand command = new SqlCommand("select budget_id from [dbo].[transaksi_budget] where budget_id in (" +String.Join(",", budgetIdsOfInputs)+ ")", conn);
            SqlDataReader dataReader = command.ExecuteReader();

            List<long> queriedBudgetIds = new List<long>();
            while(dataReader.Read()) {
                long value = Utils.obj2long(dataReader.GetValue(dataReader.GetOrdinal("budget_id")));
                queriedBudgetIds.Add(value);
            }
            dataReader.Close();
            command.Dispose();

            List<long> missingBudgetIds = new List<long>();
            foreach(RowData<ColumnName, Data> row in inputs) {
                long budget_id = Utils.obj2long(row["budget_id"]);
                if(budget_id == 0) continue;
                if(queriedBudgetIds.Contains(budget_id)) continue;
                if(!missingBudgetIds.Contains(budget_id)) {
                    missingBudgetIds.Add(budget_id);
                }
            }

            if(missingBudgetIds.Count > 0) {
                command = new SqlCommand("select jurnal_id, budget_id from [dbo].[transaksi_jurnal] where budget_id in (" + String.Join(",", missingBudgetIds) + ")", conn);
                dataReader = command.ExecuteReader();

                Dictionary<long, List<string>> missingBudgetReferencedIds = new Dictionary<long, List<string>>();
                while(dataReader.Read()) {
                    string jurnal_id = Utils.obj2str(dataReader.GetValue(dataReader.GetOrdinal("jurnal_id")));
                    long budget_id = Utils.obj2long(dataReader.GetValue(dataReader.GetOrdinal("budget_id")));
                    if(missingBudgetIds.Contains(budget_id)) {
                        if(!missingBudgetReferencedIds.ContainsKey(budget_id)) {
                            missingBudgetReferencedIds[budget_id] = new List<string>();
                        }
                        missingBudgetReferencedIds[budget_id].Add(jurnal_id);
                    }
                }
                dataReader.Close();
                command.Dispose();

                if(missingBudgetReferencedIds.Count > 0) {
                    throw new Exception("Some of transaksi_budget is missing, but being referenced in transaksi_jurnal ([budget_id -> list of jurnal_id] map): " + JsonSerializer.Serialize(missingBudgetReferencedIds));
                }

                Log.Logger.Warning("Missing transaction_budget(ids): " + JsonSerializer.Serialize(missingBudgetIds));
            }

            return missingBudgetIds;
        }

        private List<long> fillsMissingCreationDate(List<RowData<ColumnName, Data>> inputs) {
            List<long> missingCreationDateIds = new List<long>();
            List<long> budgetIdRefs = new List<long>();

            foreach(RowData<ColumnName, Data> row in inputs) {
                DateTime? budgetdetil_date = Utils.obj2datetimeNullable(row["budgetdetil_date"]);
                if(budgetdetil_date == null) {
                    long budgetdetil_id = Utils.obj2long(row["budgetdetil_id"]);
                    long budget_id = Utils.obj2long(row["budget_id"]);

                    missingCreationDateIds.Add(budgetdetil_id);
                    if(!budgetIdRefs.Contains(budget_id)) {
                        budgetIdRefs.Add(budget_id);
                    }
                }
            }

            SqlConnection conn = (SqlConnection)connections.Where(a => a.GetDbLoginInfo().dbname == "E_FRM").FirstOrDefault().GetDbConnection();
            SqlCommand command = new SqlCommand("select budget_id, budget_entrydt from [dbo].[transaksi_budget] where budget_id in (" + String.Join(",", budgetIdRefs) + ")", conn);
            SqlDataReader dataReader = command.ExecuteReader();

            Dictionary<BudgetId, RowData<ColumnName, Data>> queriedBudgets = new Dictionary<BudgetId, RowData<ColumnName, Data>>();
            while(dataReader.Read()) {
                long budget_id = Utils.obj2long(dataReader.GetValue(dataReader.GetOrdinal("budget_id")));
                DateTime budget_entrydt = Utils.obj2datetime(dataReader.GetValue(dataReader.GetOrdinal("budget_entrydt")));
                queriedBudgets[budget_id] = new RowData<ColumnName, Data>() {
                    { "budget_id", budget_id },
                    { "budget_entrydt", budget_entrydt },
                };
            }
            dataReader.Close();
            command.Dispose();

            foreach(RowData<ColumnName, Data> row in inputs) {
                DateTime? budgetdetil_date = Utils.obj2datetimeNullable(row["budgetdetil_date"]);
                if(budgetdetil_date == null) {
                    long budget_id = Utils.obj2long(row["budget_id"]);
                    row["budgetdetil_date"] = queriedBudgets[budget_id]["budget_entrydt"];
                }
            }

            return missingCreationDateIds;
        }
    }
}
