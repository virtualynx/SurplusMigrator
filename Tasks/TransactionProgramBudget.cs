using Microsoft.Data.SqlClient;
using Npgsql;
using SurplusMigrator.Interfaces;
using SurplusMigrator.Models;
using SurplusMigrator.Tasks;
using System;
using System.Collections.Generic;
using System.Linq;

namespace SurplusMigrator.Tasks {
  class TransactionProgramBudget : _BaseTask {
        public TransactionProgramBudget(DbConnection_[] connections) : base(connections) {
            sources = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().dbname == "E_FRM").FirstOrDefault(),
                    tableName = "prabudget_program",
                    columns = new string[] {
                        "prabudget_program_id",
                        "prabudget_program_title",
                        "prabudget_program_focus_id",
                        //"prabudget_program_epstotal",
                        "prabudget_program_descr",
                        "strukturunit_id",
                        "prabudget_program_pulled",
                        "created_by",
                        "created_dt",
                        "modified_by",
                        "modified_dt",
                        "prabudget_program_isdisable",
                        "prabudget_program_isdisable_by",
                        "prabudget_program_isdisable_dt",
                        "prabudget_program_islock",
                        "prabudget_program_islock_by",
                        "prabudget_program_islock_dt",
                        "prabudget_program_month",
                        "prabudget_program_year",
                        "prabudget_program_approved",
                        "prabudget_program_approved_by",
                        "prabudget_program_approved_dt",
                        "prabudget_program_type_id",
                        "prabudget_program_approved_user",
                        "prabudget_program_approved_user_by",
                        "prabudget_program_approved_user_dt",
                        "prabudget_program_reference",
                        "prabudget_program_produser_nik",
                        "showinventorycategory_id",
                        "showinventorydepartment_id",
                        "showinventorytimezone_id",
                        "prabudget_program_duration",
                        "prabudget_program_saldoeps",
                        "prabudget_program_ep_nik",
                        "prabudget_program_contenttype_id",
                    },
                    ids = new string[] { "prabudget_program_id" }
                }
            };
            destinations = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().dbname == "insosys").FirstOrDefault(),
                    tableName = "transaction_program_Budget",
                    columns = new string[] {
                        "tprogrambudgetid",
                        "descr",
                        "ispulled",
                        "islocked",
                        "lockedby",
                        "lockeddate",
                        "month",
                        "year",
                        "isapproved1",
                        "approved1by",
                        "approved1date",
                        "isapproved2",
                        "approved2by",
                        "approved2date",
                        "reference",
                        "producernik",
                        "duration",
                        "epsbalance",
                        "epnik",
                        "tvprogramname",
                        "tvprogramid",
                        "departmentid",
                        "programbudgettypeid",
                        "showinventorycategoryid",
                        "showinventorydepartmentid",
                        "showinventorytimezoneid",
                        "programbudgetcontenttypeid",
                        "created_date",
                        "created_by",
                        "is_disabled",
                        "disabled_date",
                        "disabled_by",
                        "modified_date",
                        "modified_by",
                    },
                    ids = new string[] { "tprogrambudgetid" }
                }
            };
        }

        public override List<RowData<ColumnName, Data>> getSourceData(Table[] sourceTables, int batchSize = 5000) {
            return sourceTables.Where(a => a.tableName == "prabudget_program").FirstOrDefault().getDatas(batchSize);
        }

        public override MappedData mapData(List<RowData<ColumnName, Data>> inputs) {
            MappedData result = new MappedData();

            foreach(RowData<ColumnName, Data> data in inputs) {
                int showinventorycategoryid = Utils.obj2int(data["showinventorycategory_id"]);
                int showinventorydepartmentid = Utils.obj2int(data["showinventorydepartment_id"]);
                int showinventorytimezoneid = Utils.obj2int(data["showinventorytimezone_id"]);

                RowData<ColumnName, Data> insertRow = new RowData<ColumnName, Data>() {
                    { "tprogrambudgetid",  data["prabudget_program_id"]},
                    { "descr",  data["prabudget_program_descr"]},
                    { "ispulled",  Utils.obj2bool(data["prabudget_program_pulled"])},
                    { "islocked",  Utils.obj2bool(data["prabudget_program_islock"])},
                    { "lockedby",  data["prabudget_program_islock_by"]},
                    { "lockeddate",  data["prabudget_program_islock_dt"]},
                    { "month",  data["prabudget_program_month"]},
                    { "year",  data["prabudget_program_year"]},
                    { "isapproved1",  Utils.obj2bool(data["prabudget_program_approved"])},
                    { "approved1by",  data["prabudget_program_approved_by"]},
                    { "approved1date",  data["prabudget_program_approved_dt"]},
                    { "isapproved2",  Utils.obj2bool(data["prabudget_program_approved_user"])},
                    { "approved2by",  data["prabudget_program_approved_user_by"]},
                    { "approved2date",  data["prabudget_program_approved_user_dt"]},
                    { "reference",  data["prabudget_program_reference"]},
                    { "producernik",  data["prabudget_program_produser_nik"]},
                    { "duration",  data["prabudget_program_duration"]},
                    { "epsbalance",  data["prabudget_program_saldoeps"]},
                    { "epnik",  data["prabudget_program_ep_nik"]},
                    { "tvprogramname",  data["prabudget_program_title"]},
                    { "tvprogramid",  data["prabudget_program_focus_id"]},
                    { "departmentid",  data["strukturunit_id"]},
                    { "programbudgettypeid",  data["prabudget_program_type_id"]},
                    { "showinventorycategoryid",  showinventorycategoryid!=0? showinventorycategoryid: null},
                    { "showinventorydepartmentid",  showinventorydepartmentid!=0? showinventorydepartmentid: null},
                    { "showinventorytimezoneid",  showinventorytimezoneid!=0? showinventorytimezoneid: null},
                    { "programbudgetcontenttypeid",  data["prabudget_program_contenttype_id"]},
                    { "created_date",  data["created_dt"]},
                    { "created_by",  new AuthInfo(){ FullName = Utils.obj2str(data["created_by"]) } },
                    { "is_disabled", Utils.obj2bool(data["prabudget_program_isdisable"]) },
                    { "disabled_date",  data["prabudget_program_isdisable_dt"]},
                    { "disabled_by",  new AuthInfo(){ FullName = Utils.obj2str(data["prabudget_program_isdisable_by"]) } },
                    { "modified_date",  data["modified_dt"]},
                    { "modified_by",  new AuthInfo(){ FullName = Utils.obj2str(data["modified_by"]) } },
                };
                result.addData("transaction_program_Budget", insertRow);
            }

            return result;
        }

        public override MappedData additionalStaticData() {
            return null;
        }

        public override void runDependencies() {
            new MasterProgramBudgetContenttype(connections).run();
            new MasterProgramBudgetType(connections).run();
        }
    }
}
