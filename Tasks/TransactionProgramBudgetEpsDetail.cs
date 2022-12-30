using Microsoft.Data.SqlClient;
using SurplusMigrator.Interfaces;
using SurplusMigrator.Libraries;
using SurplusMigrator.Models;
using System;
using System.Collections.Generic;
using System.Linq;

namespace SurplusMigrator.Tasks {
    class TransactionProgramBudgetEpsDetail : _BaseTask, RemappableId {
        public TransactionProgramBudgetEpsDetail(DbConnection_[] connections) : base(connections) {
            sources = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().dbname == "E_FRM").FirstOrDefault(),
                    tableName = "program_detil_eps",
                    columns = new string[] {
                        "prabudget_program_id",
                        "eps_line",
                        "eps_no",
                        "eps_proposed_title",
                        "eps_realization_title",
                        "eps_barcodetape",
                        "eps_onair_type",
                        "eps_approved",
                        "eps_status",
                        "eps_iscanceled",
                        "eps_iscanceledby",
                        "eps_iscanceleddt",
                        "eps_memo",
                        "eps_memo_descr",
                        "eps_approvedby",
                        "eps_approveddate",
                        "eps_ismastershoot",
                        "eps_mastershootby",
                        "eps_mastershootdate",
                        "eps_code",
                        "eps_settlement_title",
                        "eps_total_shift",
                        "eps_log_report",
                        "eps_isapprovedpostprod",
                        "eps_approvedpostprodby",
                        "eps_approvedpostproddate",
                        "eps_duration_minutes",
                        "eps_duration_seconds",
                        "eps_classification"
                    },
                    ids = new string[] { "prabudget_program_id", "eps_line" }
                }
            };
            destinations = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().dbname == "insosys").FirstOrDefault(),
                    tableName = "transaction_program_budget_eps_detail",
                    columns = new string[] {
                        "tprogrambudget_epsdetailid",
                        "tprogrambudgetid",
                        "no",
                        "proposedtitle",
                        "realizationtitle",
                        "tapebarcode",
                        "onairtype",
                        "isapproved",
                        "approvedby",
                        "approveddate",
                        "status",
                        "iscanceled",
                        "canceledby",
                        "canceleddate",
                        "memo",
                        "memo_descr",
                        "ismastershoot",
                        "mastershootby",
                        "mastershootdate",
                        "epscode",
                        "settlementtitle",
                        "shifttotal",
                        "logreport",
                        "isapproved_postprod",
                        "approvedby_postprod",
                        "approveddate_postprod",
                        "duration_minutes",
                        "duration_seconds",
                        "classification",
                        "created_date",
                        "created_by",
                        "is_disabled"
                    },
                    ids = new string[] { "tprogrambudget_epsdetailid" }
                }
            };
        }

        protected override List<RowData<ColumnName, object>> getSourceData(Table[] sourceTables, int batchSize = defaultReadBatchSize) {
            return sourceTables.Where(a => a.tableName == "program_detil_eps").FirstOrDefault().getDatas(batchSize);
        }

        protected override MappedData mapData(List<RowData<ColumnName, object>> inputs) {
            MappedData result = new MappedData();

            Dictionary<string, Dictionary<string, object>> createdDateMaps = getCreatedInfoMap(inputs);

            foreach(RowData<ColumnName, object> data in inputs) {
                string prabudget_program_id = Utils.obj2str(data["prabudget_program_id"]);
                DateTime created_dt = (DateTime)createdDateMaps[prabudget_program_id]["created_dt"];
                string tprogrambudget_epsdetailid = SequencerString.getId("PBEPS", created_dt);
                int line = Utils.obj2int(data["eps_line"]);
                string mappingTag = prabudget_program_id + "-" + line.ToString();
                IdRemapper.add("tprogrambudget_epsdetailid", mappingTag, tprogrambudget_epsdetailid);

                result.addData(
                    "transaction_program_budget_eps_detail",
                    new RowData<ColumnName, object>() {
                        { "tprogrambudget_epsdetailid",  tprogrambudget_epsdetailid},
                        { "tprogrambudgetid",  data["prabudget_program_id"]},
                        { "no", Utils.obj2int(data["eps_no"])},
                        { "proposedtitle",  data["eps_proposed_title"]},
                        { "realizationtitle",  data["eps_realization_title"]},
                        { "tapebarcode",  data["eps_barcodetape"]},
                        { "onairtype",  data["eps_onair_type"]},
                        { "isapproved", Utils.obj2bool(data["eps_approved"])},
                        { "approvedby",  data["eps_approvedby"]},
                        { "approveddate", Utils.obj2datetimeNullable(data["eps_approveddate"])},
                        { "status", Utils.obj2int(data["eps_status"])},
                        { "iscanceled", Utils.obj2bool(data["eps_iscanceled"])},
                        { "canceledby",  data["eps_iscanceledby"]},
                        { "canceleddate", Utils.obj2datetimeNullable(data["eps_iscanceleddt"])},
                        { "memo",  data["eps_memo"]},
                        { "memo_descr",  data["eps_memo_descr"]},
                        { "ismastershoot", Utils.obj2bool(data["eps_ismastershoot"])},
                        { "mastershootby",  data["eps_mastershootby"]},
                        { "mastershootdate", Utils.obj2datetimeNullable(data["eps_mastershootdate"])},
                        { "epscode",  data["eps_code"]},
                        { "settlementtitle",  data["eps_settlement_title"]},
                        { "shifttotal", Utils.obj2int(data["eps_total_shift"])},
                        { "logreport",  data["eps_log_report"]},
                        { "isapproved_postprod", Utils.obj2bool(data["eps_isapprovedpostprod"])},
                        { "approvedby_postprod",  data["eps_approvedpostprodby"]},
                        { "approveddate_postprod", Utils.obj2datetimeNullable(data["eps_approvedpostproddate"])},
                        { "duration_minutes", Utils.obj2int(data["eps_duration_minutes"])},
                        { "duration_seconds", Utils.obj2int(data["eps_duration_seconds"])},
                        { "classification",  data["eps_classification"]},
                        { "created_date", created_dt},
                        { "created_by", getAuthInfo(createdDateMaps[prabudget_program_id]["created_by"]) },
                        { "is_disabled", false}
                    }
                );
            }

            return result;
        }

        private Dictionary<string, Dictionary<string, object>> getCreatedInfoMap(List<RowData<ColumnName, object>> inputs) {
            Dictionary<string, Dictionary<string, object>> result = new Dictionary<string, Dictionary<string, object>>();

            List<string> prabudgetProgramIds = new List<string>();
            foreach(RowData<ColumnName, object> row in inputs) {
                string prabudgetProgramId = Utils.obj2str(row["prabudget_program_id"]);
                if(!prabudgetProgramIds.Contains(prabudgetProgramId)) {
                    prabudgetProgramIds.Add(prabudgetProgramId);
                }
            }

            SqlConnection conn = (SqlConnection)connections.Where(a => a.GetDbLoginInfo().dbname == "E_FRM").FirstOrDefault().GetDbConnection();
            SqlCommand command = new SqlCommand("select prabudget_program_id, created_by, created_dt from [dbo].[prabudget_program] where prabudget_program_id in ('" + String.Join("','", prabudgetProgramIds) + "')", conn);
            SqlDataReader dataReader = command.ExecuteReader();

            while(dataReader.Read()) {
                string prabudget_program_id = Utils.obj2str(dataReader.GetValue(dataReader.GetOrdinal("prabudget_program_id")));
                string created_by = Utils.obj2str(dataReader.GetValue(dataReader.GetOrdinal("created_by")));
                DateTime created_dt = Utils.obj2datetime(dataReader.GetValue(dataReader.GetOrdinal("created_dt")));
                Dictionary<string, object> data = new Dictionary<string, object>();
                data.Add("created_by", created_by);
                data.Add("created_dt", created_dt);

                result.Add(prabudget_program_id, data);
            }
            dataReader.Close();
            command.Dispose();

            return result;
        }

        protected override void afterFinishedCallback() {
            IdRemapper.saveMap();
        }

        public void clearRemappingCache() {
            IdRemapper.clearMapping("tprogrambudget_epsdetailid");
        }

        protected override void runDependencies() {
            new TransactionProgramBudget(connections).run();
        }
    }
}
