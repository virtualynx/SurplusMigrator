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
                        //"ref_line",
                        //"ref_budgetline",
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

        public override List<RowData<ColumnName, Data>> getSourceData(Table[] sourceTables, int batchSize = 5000) {
            return sourceTables.Where(a => a.tableName == "transaksi_jurnaldetil").FirstOrDefault().getDatas(batchSize);
        }

        public override MappedData mapData(List<RowData<ColumnName, Data>> inputs) {
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

            foreach(RowData<ColumnName, Data> data in inputs) {
                string tjournal_detailid = Sequencer.getId(getJournalIdPrefix(data["jurnal_id"].ToString()) +"D", Utils.obj2datetime(data["created_dt"]));
                string tbudgetid = null;
                if(Utils.obj2long(data["budget_id"]) > 0) {
                    tbudgetid = IdRemapper.get("tbudgetid", data["budget_id"]).ToString();
                }
                string tbudget_detailid = null;
                if(Utils.obj2long(data["budgetdetil_id"]) > 0) {
                    tbudget_detailid = IdRemapper.get("tbudget_detailid", Utils.obj2long(data["budgetdetil_id"])).ToString();
                }

                result.addData(
                    "transaction_journal_detail",
                    new RowData<ColumnName, Data>() {
                        { "tjournal_detailid",  tjournal_detailid},
                        { "tjournalid",  data["jurnal_id"]},
                        { "dk",  data["jurnaldetil_dk"]},
                        { "description",  data["jurnaldetil_descr"]},
                        { "foreignamount",  Utils.obj2decimal(data["jurnaldetil_foreign"])},
                        { "foreignrate",  Utils.obj2decimal(data["jurnaldetil_foreignrate"])},
                        { "ref_detail_id",  null}, //ref_line ?
                        { "ref_subdetail_id",  0}, //ref_budgetline ?
                        { "vendorid",  Utils.obj2long(data["rekanan_id"])==0? null: data["rekanan_id"]},
                        { "accountid",  Utils.obj2long(data["acc_id"])==0? null: data["acc_id"]},
                        { "currencyid",  data["currency_id"]},
                        { "departmentid",  data["strukturunit_id"]},
                        { "tbudgetid",  tbudgetid},
                        { "tbudget_detailid",  tbudget_detailid},
                        { "ref_id",  data["ref_id"]},
                        { "bilyet_no",  null},
                        { "bilyet_date",  null},
                        { "bilyet_effectivedate",  null},
                        { "received_by",  null},
                        { "created_date",  data["created_dt"]},
                        { "created_by",  new AuthInfo(){ FullName = Utils.obj2str(data["created_by"]) } },
                        { "is_disabled", Utils.obj2bool(data["jurnal_isdisabled"])},
                        { "disabled_date",  data["jurnal_isdisableddt"]},
                        { "disabled_by",  new AuthInfo(){ FullName = Utils.obj2str(data["jurnal_isdisabledby"]) } },
                        { "modified_date",  null},
                        { "modified_by",  null},
                        //{ "budgetdetail_name",  data[""]},
                        { "idramount",  data["jurnaldetil_idr"]},
                        { "bankaccountid",  null},
                        { "paymenttypeid",  0},
                        { "journalreferencetypeid",  getJournalReferenceTypeId(data["jurnal_id"].ToString())},
                        { "subreference_id",  null},
                    }
                );
            }

            return result;
        }

        public override MappedData additionalStaticData() {
            return null;
        }

        public override void runDependencies() {
            //new TransactionBudgetDetail(connections).run(true, 3855);
        }

        private string getJournalIdPrefix(string tjournalid) {
            Match match = Regex.Match(tjournalid, @"[a-zA-Z]+");

            return match.Groups[0].Value;
        }

        private string getJournalReferenceTypeId(string tjournalid) {
            Dictionary<string, string> referenceTypeMap = new Dictionary<string, string>() {
                { "AP", "Jurnal AP" },
                { "CN", null },
                { "DN", null },
                { "JV", "Jurnal JV" },
                { "OC", null },
                { "OR", null },
                { "PV", "Payment" },
                { "RV", null },
                { "SA", null },
                { "ST", "Settlement" },
            };

            return referenceTypeMap[getJournalIdPrefix(tjournalid)];
        }

        private void addTrackingFields(List<RowData<ColumnName, Data>> inputs) {
            List<string> journalIds = new List<string>();

            foreach(RowData<ColumnName, Data> row in inputs) {
                string jurnal_id = Utils.obj2str(row["jurnal_id"]);
                if(!journalIds.Contains(jurnal_id)) {
                    journalIds.Add(jurnal_id);
                }
            }

            SqlConnection conn = (SqlConnection)connections.Where(a => a.GetDbLoginInfo().dbname == "E_FRM").FirstOrDefault().GetDbConnection();
            SqlCommand command = new SqlCommand("select jurnal_id, created_dt, created_by, jurnal_isdisabled, jurnal_isdisableddt, jurnal_isdisabledby from [dbo].[transaksi_jurnal] where jurnal_id in ('" + String.Join("','", journalIds) + "')", conn);
            SqlDataReader dataReader = command.ExecuteReader();

            Dictionary<JournalId, RowData<ColumnName, Data>> queriedJournals = new Dictionary<JournalId, RowData<ColumnName, Data>>();
            while(dataReader.Read()) {
                string journalId = Utils.obj2str(dataReader.GetValue(dataReader.GetOrdinal("jurnal_id")));
                queriedJournals[journalId] = new RowData<ColumnName, Data>() {
                    { "created_dt", dataReader.GetValue(dataReader.GetOrdinal("created_dt")) },
                    { "created_by", dataReader.GetValue(dataReader.GetOrdinal("created_by")) },
                    { "jurnal_isdisabled", dataReader.GetValue(dataReader.GetOrdinal("jurnal_isdisabled")) },
                    { "jurnal_isdisableddt", dataReader.GetValue(dataReader.GetOrdinal("jurnal_isdisableddt")) },
                    { "jurnal_isdisabledby", dataReader.GetValue(dataReader.GetOrdinal("jurnal_isdisabledby")) },
                };
            }
            dataReader.Close();
            command.Dispose();

            foreach(RowData<ColumnName, Data> row in inputs) {
                RowData<ColumnName, Data> journal = queriedJournals[row["jurnal_id"].ToString()];
                row["created_dt"] = journal["created_dt"];
                row["created_by"] = journal["created_by"];
                row["jurnal_isdisabled"] = journal["jurnal_isdisabled"];
                row["jurnal_isdisableddt"] = journal["jurnal_isdisableddt"];
                row["jurnal_isdisabledby"] = journal["jurnal_isdisabledby"];
            }
        }
    }
}
