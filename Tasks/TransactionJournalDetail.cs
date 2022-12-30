using Microsoft.Data.SqlClient;
using Npgsql;
using SurplusMigrator.Libraries;
using SurplusMigrator.Models;
using SurplusMigrator.Tasks;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;

using JournalId = System.String;
using JournalDetailId = System.String;
using BudgetId = System.Int64;
using BudgetDetailId = System.Int64;
using Serilog;
using System.Text.Json;
using SurplusMigrator.Models.Others;
using System.IO;

namespace SurplusMigrator.Tasks {
  class TransactionJournalDetail : _BaseTask {
        public TransactionJournalDetail(DbConnection_[] connections) : base(connections) {
            sources = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().dbname == "E_FRM").FirstOrDefault(),
                    tableName = "transaksi_jurnaldetil",
                    columns = new string[] {
                        "jurnal_id",
                        "jurnaldetil_line",
                        "jurnaldetil_dk",
                        "jurnaldetil_descr",
                        "rekanan_id",
                        //"rekanan_name",
                        "acc_id",
                        "currency_id",
                        "jurnaldetil_foreign",
                        "jurnaldetil_foreignrate",
                        "jurnaldetil_idr",
                        //"channel_id",
                        "strukturunit_id",
                        "ref_id",
                        "ref_line",
                        "ref_budgetline",
                        //"region_id",
                        //"branch_id",
                        "budget_id",
                        //"budget_name",
                        "budgetdetil_id",
                        //"budgetdetil_name",
                    },
                    ids = new string[] { "jurnal_id", "jurnaldetil_line" }
                }
            };
            destinations = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().dbname == "insosys").FirstOrDefault(),
                    tableName = "transaction_journal_detail",
                    columns = new string[] {
                        "tjournal_detailid",
                        "tjournalid",
                        "dk",
                        "description",
                        "foreignamount",
                        "foreignrate",
                        "ref_detail_id",
                        "ref_subdetail_id",
                        "vendorid",
                        "accountid",
                        "currencyid",
                        "departmentid",
                        "tbudgetid",
                        "tbudget_detailid",
                        "ref_id",
                        "bilyet_no",
                        "bilyet_date",
                        "bilyet_effectivedate",
                        "received_by",
                        "created_date",
                        "created_by",
                        "is_disabled",
                        "disabled_date",
                        "disabled_by",
                        "modified_date",
                        "modified_by",
                        //"budgetdetail_name", removed
                        "idramount",
                        "bankaccountid",
                        "paymenttypeid",
                        "journalreferencetypeid",
                        "subreference_id",
                    },
                    ids = new string[] { "tjournal_detailid" }
                }
            };
        }

        protected override List<RowData<ColumnName, object>> getSourceData(Table[] sourceTables, int batchSize = defaultReadBatchSize) {
            return sourceTables.Where(a => a.tableName == "transaksi_jurnaldetil").FirstOrDefault().getDatas(batchSize);
        }

        protected override MappedData mapData(List<RowData<ColumnName, object>> inputs) {
            MappedData result = new MappedData();

            addTrackingFields(inputs);
            nullifyMissingReferences(
                "budget_id",
                "transaksi_budget",
                "budget_id",
                connections.Where(a => a.GetDbLoginInfo().dbname == "E_FRM").FirstOrDefault(),
                inputs
            );
            nullifyMissingReferences(
                "budgetdetil_id",
                "transaksi_budgetdetil",
                "budgetdetil_id",
                connections.Where(a => a.GetDbLoginInfo().dbname == "E_FRM").FirstOrDefault(),
                inputs
            );
            nullifyMissingReferences(
                "rekanan_id",
                "master_rekanan",
                "rekanan_id",
                connections.Where(a => a.GetDbLoginInfo().dbname == "E_FRM").FirstOrDefault(),
                inputs
            );
            nullifyMissingReferences(
                "acc_id",
                "master_acc",
                "acc_id",
                connections.Where(a => a.GetDbLoginInfo().dbname == "E_FRM").FirstOrDefault(),
                inputs
            );

            foreach(RowData<ColumnName, object> data in inputs) {
                string tjournal_detailid = data["jurnal_id"].ToString().Substring(0, 2)+"D"+ data["jurnal_id"].ToString().Substring(2)+ data["jurnaldetil_line"].ToString();
                
                string tbudgetid = null;
                //if(Utils.obj2long(data["budget_id"]) > 0) {
                //    tbudgetid = IdRemapper.get("tbudgetid", data["budget_id"]).ToString();
                //}
                tbudgetid = Utils.obj2str(data["budget_id"]);
                tbudgetid = tbudgetid == "0" ? null : tbudgetid;

                string tbudget_detailid = null;
                //if(Utils.obj2long(data["budgetdetil_id"]) > 0) {
                //    tbudget_detailid = IdRemapper.get("tbudget_detailid", Utils.obj2long(data["budgetdetil_id"])).ToString();
                //}
                tbudget_detailid = Utils.obj2str(data["budgetdetil_id"]);
                tbudget_detailid = tbudget_detailid == "0" ? null : tbudget_detailid;

                string tjournalid = Utils.obj2str(data["jurnal_id"]).ToUpper();
                string accountid = Utils.obj2str(data["acc_id"]);
                accountid = accountid == "0" ? null : accountid;
                
                string ref_id = Utils.obj2str(data["ref_id"]);
                string ref_line = Utils.obj2str(data["ref_line"]);
                string ref_detail_id = null;
                if((ref_id != null && ref_id != "0") && ref_line != null) {
                    ref_detail_id = ref_id.Substring(0, 2) + "D" + ref_id.Substring(2) + ref_line;
                }

                string ref_budgetline = Utils.obj2str(data["ref_budgetline"]);
                string ref_subdetail_id = null;
                if(ref_detail_id != null && ref_budgetline != null) {
                    ref_subdetail_id = ref_detail_id + "_" + ref_budgetline;
                }

                result.addData(
                    "transaction_journal_detail",
                    new RowData<ColumnName, object>() {
                        { "tjournal_detailid",  tjournal_detailid},
                        { "tjournalid",  tjournalid},
                        { "dk",  data["jurnaldetil_dk"]},
                        { "description",  data["jurnaldetil_descr"]},
                        { "foreignamount",  Utils.obj2decimal(data["jurnaldetil_foreign"])},
                        { "foreignrate",  Utils.obj2decimal(data["jurnaldetil_foreignrate"])},
                        { "vendorid",  Utils.obj2long(data["rekanan_id"])==0? null: data["rekanan_id"]},
                        { "accountid",  accountid},
                        { "currencyid",  data["currency_id"]},
                        { "departmentid",  data["strukturunit_id"]},
                        { "tbudgetid",  tbudgetid},
                        { "tbudget_detailid",  tbudget_detailid},
                        { "ref_id",  data["ref_id"]},
                        { "ref_detail_id",  ref_detail_id},
                        { "ref_subdetail_id",  Utils.obj2int(data["ref_line"])},
                        { "bilyet_no",  null},
                        { "bilyet_date",  null},
                        { "bilyet_effectivedate",  null},
                        { "received_by",  null},
                        { "created_date",  data["created_dt"]},
                        { "created_by", getAuthInfo(data["created_by"], true) },
                        { "is_disabled", Utils.obj2bool(data["jurnal_isdisabled"]) },
                        { "disabled_date",  data["jurnal_isdisableddt"]},
                        { "disabled_by", getAuthInfo(data["jurnal_isdisabledby"]) },
                        { "modified_date",  null},
                        { "modified_by",  null},
                        //{ "budgetdetail_name",  data[""]},
                        { "idramount",  data["jurnaldetil_idr"]},
                        { "bankaccountid",  null},
                        { "paymenttypeid",  0},
                        { "journalreferencetypeid",  getJournalReferenceTypeId(tjournalid)},
                        { "subreference_id",  null},
                    }
                );
            }

            return result;
        }

        protected override void runDependencies() {
            new MasterBankAccount(connections).run();
            new MasterJournalReferenceType(connections).run();
            new TransactionBudgetDetail(connections).run(true);
        }

        private string getJournalIdPrefix(string tjournalid) {
            Match match = Regex.Match(tjournalid, @"[a-zA-Z]+");

            return match.Groups[0].Value;
        }

        private string getJournalReferenceTypeId(string tjournalid) {
            Dictionary<string, string> referenceTypeMap = new Dictionary<string, string>() {
                { "AP", "jurnal_ap" },
                { "CN", null },
                { "DN", null },
                { "JV", "jurnal_jv" },
                { "OC", null },
                { "OR", null },
                { "PV", "payment" },
                { "RV", null },
                { "SA", null },
                { "ST", "payment" },
            };

            return referenceTypeMap[getJournalIdPrefix(tjournalid)];
        }

        private void addTrackingFields(List<RowData<ColumnName, object>> inputs) {
            List<string> journalIds = new List<string>();

            foreach(RowData<ColumnName, object> row in inputs) {
                string jurnal_id = Utils.obj2str(row["jurnal_id"]);
                if(!journalIds.Contains(jurnal_id)) {
                    journalIds.Add(jurnal_id);
                }
            }

            SqlConnection conn = (SqlConnection)connections.Where(a => a.GetDbLoginInfo().dbname == "E_FRM").FirstOrDefault().GetDbConnection();
            SqlCommand command = new SqlCommand("select jurnal_id, created_dt, created_by, jurnal_isdisabled, jurnal_isdisableddt, jurnal_isdisabledby from [dbo].[transaksi_jurnal] where jurnal_id in ('" + String.Join("','", journalIds) + "')", conn);
            SqlDataReader dataReader = command.ExecuteReader();

            Dictionary<JournalId, RowData<ColumnName, object>> queriedJournals = new Dictionary<JournalId, RowData<ColumnName, object>>();
            while(dataReader.Read()) {
                string journalId = Utils.obj2str(dataReader.GetValue(dataReader.GetOrdinal("jurnal_id"))).ToUpper();
                queriedJournals[journalId] = new RowData<ColumnName, object>() {
                    { "created_dt", dataReader.GetValue(dataReader.GetOrdinal("created_dt")) },
                    { "created_by", dataReader.GetValue(dataReader.GetOrdinal("created_by")) },
                    { "jurnal_isdisabled", dataReader.GetValue(dataReader.GetOrdinal("jurnal_isdisabled")) },
                    { "jurnal_isdisableddt", dataReader.GetValue(dataReader.GetOrdinal("jurnal_isdisableddt")) },
                    { "jurnal_isdisabledby", dataReader.GetValue(dataReader.GetOrdinal("jurnal_isdisabledby")) },
                };
            }
            dataReader.Close();
            command.Dispose();

            foreach(RowData<ColumnName, object> row in inputs) {
                RowData<ColumnName, object> journal = queriedJournals[Utils.obj2str(row["jurnal_id"]).ToUpper()];
                row["created_dt"] = journal["created_dt"];
                row["created_by"] = journal["created_by"];
                row["jurnal_isdisabled"] = journal["jurnal_isdisabled"];
                row["jurnal_isdisableddt"] = journal["jurnal_isdisableddt"];
                row["jurnal_isdisabledby"] = journal["jurnal_isdisabledby"];
            }
        }
    }
}
