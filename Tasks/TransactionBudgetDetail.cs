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
using SurplusMigrator.Interfaces;
using System.Text.RegularExpressions;
using System.IO;

namespace SurplusMigrator.Tasks {
    class TransactionBudgetDetail : _BaseTask, RemappableId {
        /**
         * here holds ids of the missing TransactionBudget data
         * these TransactionBudget data also does not referred anywhere on TransactionJournal
         * so we can safely assumed that theyre safe to be ignored
         */
        private static List<long> allMissingBudgetIds = new List<long>();

        /**
         * here holds ids of the TransactionBudgetDetail data which has null reference to budget_id and also has null creation-date
         * these TransactionBudgetDetail data also does not referred anywhere on TransactionJournalDetail
         * so we can safely assumed that theyre safe to be ignored
         */
        private static List<long> allUnreferencedDataIds = new List<long>();

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

        protected override List<RowData<ColumnName, object>> getSourceData(Table[] sourceTables, int batchSize = defaultReadBatchSize) {
            return sourceTables.Where(a => a.tableName == "transaksi_budgetdetil").FirstOrDefault().getDatas(batchSize);
        }

        protected override MappedData mapData(List<RowData<ColumnName, object>> inputs) {
            MappedData result = new MappedData();

            ///*
            /// there is some changes of ids on MasterBudgetAccount, so we also need-
            /// to change all reference to the respective changed ids.
            /// see Task MasterBudgetAccount for more detailed mapping
            Dictionary<BudgetId, BudgetAccountId> remappedBudgetAccounts = new Dictionary<BudgetId, BudgetAccountId>() {
                { 666026, "5040602201" },
                { 709454, "5040601001" },
            };

            List<DbInsertFail> missingRefErrors = skipsIfMissingReferences(
                "budget_id",
                "transaksi_budget",
                "budget_id",
                connections.Where(a => a.GetDbLoginInfo().dbname == "E_FRM").FirstOrDefault(),
                inputs
            );
            if(missingRefErrors.Count > 0) {
                foreach(DbInsertFail err in missingRefErrors) {
                    Match match = Regex.Match(err.info, "(.*)key \\((.*)\\)=\\((.*)\\)");
                    string column = match.Groups[2].Value;
                    string id = match.Groups[3].Value;
                    allMissingBudgetIds.Add(Utils.obj2long(id));
                    result.addError("transaction_budget_detail", err);
                }
            }
            if(allMissingBudgetIds.Count > 0) {
                MyConsole.Warning("Total count of ignored data caused by \"missing data in table [transaksi_budget]\": " + allMissingBudgetIds.Count);
            }

            List<DbInsertFail> unreferencedErrors = skipsIfUnreferenced(inputs);
            if(unreferencedErrors.Count > 0) {
                foreach(DbInsertFail err in unreferencedErrors) {
                    Match match = Regex.Match(err.info, "Data of \\[transaksi_budgetdetil\\] Key \\((.*)\\)=\\((.*)\\)(.*)");
                    string column = match.Groups[1].Value;
                    string id = match.Groups[2].Value;
                    allUnreferencedDataIds.Add(Utils.obj2long(id));
                    result.addError("transaction_budget_detail", err);
                }
            }
            if(allUnreferencedDataIds.Count > 0) {
                MyConsole.Warning("Total count of ignored data caused by \"having null column [budget_id] & [budgetdetil_date]\": " + allUnreferencedDataIds.Count);
            }

            List<long> missingCreationDateIds = fillsMissingCreationDate(inputs);

            foreach(RowData<ColumnName, object> data in inputs) {
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
                    new RowData<ColumnName, object>() {
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

        protected override void runDependencies() {
            new MasterBudgetAccount(connections).run();
        }

        private List<DbInsertFail> skipsIfUnreferenced(List<RowData<ColumnName, object>> inputs) {
            List<DbInsertFail> result = new List<DbInsertFail>();
            List<long> nullBudgetAndDateIds = new List<long>();

            foreach(RowData<ColumnName, object> row in inputs) {
                long budget_id = Utils.obj2long(row["budget_id"]);
                DateTime? budgetdetil_date = Utils.obj2datetimeNullable(row["budgetdetil_date"]);
                if(budget_id == 0 && budgetdetil_date == null) {
                    nullBudgetAndDateIds.Add(Utils.obj2long(row["budgetdetil_id"]));
                }
            }

            if(nullBudgetAndDateIds.Count > 0) {
                //check if the data is actually being referenced in skipsIfUnreferenced
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
                //end of - check if the data is actually being referenced in skipsIfUnreferenced

                string filename = "log_(" + this.GetType().Name + ")_skipped_unreferenced_data_" + _startedAt.ToString("yyyyMMdd_HHmmss") + ".json";
                string savePath = System.Environment.CurrentDirectory + "\\" + filename;
                List<long> savedNullBudgetAndDateIds = new List<long>();
                if(File.Exists(savePath)) {
                    using(StreamReader r = new StreamReader(savePath)) {
                        string jsonText = r.ReadToEnd();
                        savedNullBudgetAndDateIds = JsonSerializer.Deserialize<List<long>>(jsonText);
                    }
                }

                savedNullBudgetAndDateIds.AddRange(nullBudgetAndDateIds);
                File.WriteAllText(savePath, JsonSerializer.Serialize(savedNullBudgetAndDateIds));
                //MyConsole.Warning("Skipped unreferenced data is found, see " + filename + " for more info");
                inputs.RemoveAll(row => nullBudgetAndDateIds.Any(nullId => row.Any(map => map.Key == "budgetdetil_id" && Utils.obj2long(map.Value) == Utils.obj2long(nullId))));

                foreach(long id in nullBudgetAndDateIds) {
                    result.Add(new DbInsertFail() {
                        info = "Data of [transaksi_budgetdetil] Key (budgetdetil_id)=("+id+") has budget_id=null, budgetdetil_date=null, and also not referenced in [transaksi_jurnaldetil]",
                        status = DbInsertFail.DB_FAIL_SEVERITY_ERROR,
                        loggedInFilename = filename
                    });
                }
            }

            return result;
        }

        private List<long> fillsMissingCreationDate(List<RowData<ColumnName, object>> inputs) {
            List<long> missingCreationDateIds = new List<long>();
            List<long> budgetIdRefs = new List<long>();

            foreach(RowData<ColumnName, object> row in inputs) {
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

            Dictionary<BudgetId, RowData<ColumnName, object>> queriedBudgets = new Dictionary<BudgetId, RowData<ColumnName, object>>();
            while(dataReader.Read()) {
                long budget_id = Utils.obj2long(dataReader.GetValue(dataReader.GetOrdinal("budget_id")));
                DateTime budget_entrydt = Utils.obj2datetime(dataReader.GetValue(dataReader.GetOrdinal("budget_entrydt")));
                queriedBudgets[budget_id] = new RowData<ColumnName, object>() {
                    { "budget_id", budget_id },
                    { "budget_entrydt", budget_entrydt },
                };
            }
            dataReader.Close();
            command.Dispose();

            foreach(RowData<ColumnName, object> row in inputs) {
                DateTime? budgetdetil_date = Utils.obj2datetimeNullable(row["budgetdetil_date"]);
                if(budgetdetil_date == null) {
                    long budget_id = Utils.obj2long(row["budget_id"]);
                    row["budgetdetil_date"] = queriedBudgets[budget_id]["budget_entrydt"];
                }
            }

            return missingCreationDateIds;
        }

        public void clearRemappingCache() {
            IdRemapper.clearMapping("tbudget_detailid");
        }
    }
}
