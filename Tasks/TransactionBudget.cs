using Microsoft.Data.SqlClient;
using Npgsql;
using Serilog;
using SurplusMigrator.Interfaces;
using SurplusMigrator.Models;
using SurplusMigrator.Tasks;
using System;
using System.Collections.Generic;
using System.Linq;

namespace SurplusMigrator.Tasks {
  class TransactionBudget : _BaseTask {
        public TransactionBudget(DbConnection_[] connections) : base(connections) {
            sources = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().dbname == "E_FRM").FirstOrDefault(),
                    tableName = "transaksi_budget",
                    columns = new string[] {
                        "budget_id",
                        "budget_name",
                        //"budget_nameshort",
                        "budget_datestart",
                        "budget_dateend",
                        "budget_isactive",
                        "budget_amount",
                        "budget_valas",
                        "currency_id",
                        "budget_amountpaid",
                        "budget_valaspaid",
                        "budget_amountreq",
                        "budget_valasreq",
                        //"budget_eps",
                        //"category_id",
                        //"showtype_id",
                        "projecttype_id",
                        //"telecast_id",
                        "strukturunit_id",
                        "budget_apprby",
                        "budget_apprdt",
                        "budget_apprauth",
                        "budget_apprver",
                        "budget_apprreq",
                        "rekanan_idproducer",
                        "prodtype_id",
                        "budget_entrybyERP",
                        "budget_entrydt",
                        "budget_ispilot",
                        //"budget_status",
                        //"project_id",
                        //"channel_id",
                        "budget_entryby",
                        //"budget_epsstart",
                        //"budget_epsend",
                        "budget_disabledby",
                        "budget_disabledate",
                        "show_id",
                        "budget_sinopsys",
                        "budget_lasteditby",
                        "budget_lasteditdt",
                        "budget_requestby",
                        "showinventorycategory_id",
                        "showinventorydepartment_id",
                        "showinventorytimezone_id",
                        "budget_ismultysystem",
                        "budget_isefp",
                        "budget_iseng",
                        "budget_islive",
                        "budget_isliputan",
                        "budget_istaping",
                        "budget_isoffair",
                        "budget_isoutdoor",
                        "budget_isindoor",
                        "budget_shootingstudio",
                        "budget_shootingdlk",
                        //"budget_epsnew",
                        //"budget_epsrepackage",
                        "budget_month",
                        "budget_year",
                        "budget_planamount",
                    },
                    ids = new string[] { "budget_id" }
                }
            };
            destinations = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().dbname == "insosys").FirstOrDefault(),
                    tableName = "transaction_budget",
                    columns = new string[] {
                        "tbudgetid",
                        "name",
                        "startdate",
                        "enddate",
                        "amount",
                        "valas",
                        "currencyid",
                        "paidamount",
                        "paidvalas",
                        "reqamount",
                        "reqvalas",
                        "tvprogramtypeid",
                        "projecttypeid",
                        "apprauth",
                        "apprver",
                        "apprreq",
                        "erpentryby",
                        "erpentrydate",
                        "ispilot",
                        "synopsis",
                        "requestedby",
                        "ismultisystem",
                        "isefp",
                        "iseng",
                        "islive",
                        "iscoverage",
                        "istapping",
                        "isoffair",
                        "isoutdoor",
                        "isindoor",
                        "shootingstudio",
                        "shootingdlk",
                        "month",
                        "year",
                        "amountplanned",
                        "departmentid",
                        "vendorproducer_id",
                        "prodtypeid",
                        "tvprogramid",
                        "showinventorycategoryid",
                        "showinventorydepartmentid",
                        "showinventorytimezoneid",
                        "created_date",
                        "created_by",
                        "is_disabled",
                        "disabled_date",
                        "disabled_by",
                        "modified_date",
                        "modified_by",
                        "tprogrambudgetid",
                        "apprauthby",
                        "apprauthdate",
                        "apprreqby",
                        "apprreqdate",
                        "apprverby",
                        "apprverdate",
                    },
                    ids = new string[] { "tbudgetid" }
                }
            };
        }

        public override List<RowData<ColumnName, Data>> getSourceData(Table[] sourceTables, int batchSize = 5000) {
            return sourceTables.Where(a => a.tableName == "transaksi_budget").FirstOrDefault().getDatas(batchSize);
        }

        public override MappedData mapData(List<RowData<ColumnName, Data>> inputs) {
            MappedData result = new MappedData();

            foreach(RowData<ColumnName, Data> data in inputs) {
                string tbudgetid = Sequencer.getId("BGT", Utils.obj2datetime(data["budget_entrydt"]));
                RemappedId.add("tbudgetid", data["budget_id"], tbudgetid);

                result.addData(
                    "transaction_budget",
                    new RowData<ColumnName, Data>() {
                        { "tbudgetid",  tbudgetid},
                        { "name",  data["budget_name"]},
                        { "startdate",  data["budget_datestart"]},
                        { "enddate",  data["budget_dateend"]},
                        { "amount",  data["budget_amount"]},
                        { "valas",  data["budget_valas"]},
                        { "currencyid",  data["currency_id"]},
                        { "paidamount",  data["budget_amountpaid"]},
                        { "paidvalas",  data["budget_valaspaid"]},
                        { "reqamount",  data["budget_amountreq"]},
                        { "reqvalas",  data["budget_valasreq"]},
                        { "tvprogramtypeid",  "PG"},
                        { "projecttypeid",  Utils.obj2int(data["projecttype_id"])==0? null: data["projecttype_id"]},
                        { "apprauth",  data["budget_apprauth"]},
                        { "apprver",  data["budget_apprver"]},
                        { "apprreq",  data["budget_apprreq"]},
                        { "erpentryby",  data["budget_entrybyERP"]},
                        { "erpentrydate",  null},
                        { "ispilot",  Utils.obj2bool(data["budget_ispilot"])},
                        { "synopsis",  data["budget_sinopsys"]},
                        { "requestedby",  data["budget_requestby"]},
                        { "ismultisystem",  Utils.obj2bool(data["budget_ismultysystem"])},
                        { "isefp",  Utils.obj2bool(data["budget_isefp"])},
                        { "iseng",  Utils.obj2bool(data["budget_iseng"])},
                        { "islive",  Utils.obj2bool(data["budget_islive"])},
                        { "iscoverage",  Utils.obj2bool(data["budget_isliputan"])},
                        { "istapping",  Utils.obj2bool(data["budget_istaping"])},
                        { "isoffair",  Utils.obj2bool(data["budget_isoffair"])},
                        { "isoutdoor",  Utils.obj2bool(data["budget_isoutdoor"])},
                        { "isindoor",  Utils.obj2bool(data["budget_isindoor"])},
                        { "shootingstudio",  data["budget_shootingstudio"]},
                        { "shootingdlk",  data["budget_shootingdlk"]},
                        { "month",  data["budget_month"]},
                        { "year",  data["budget_year"]},
                        { "amountplanned",  data["budget_planamount"]},
                        { "departmentid",  data["strukturunit_id"]},
                        { "vendorproducer_id",  Utils.obj2int(data["rekanan_idproducer"])==0? null: data["rekanan_idproducer"]},
                        { "prodtypeid",  data["prodtype_id"]},
                        { "tvprogramid",  data["show_id"]},
                        { "showinventorycategoryid",  Utils.obj2int(data["showinventorycategory_id"])==0? null: data["showinventorycategory_id"]},
                        { "showinventorydepartmentid",  Utils.obj2int(data["showinventorydepartment_id"])==0? null: data["showinventorydepartment_id"]},
                        { "showinventorytimezoneid",  Utils.obj2int(data["showinventorytimezone_id"])==0? null: data["showinventorytimezone_id"]},
                        { "created_date",  data["budget_entrydt"]},
                        { "created_by",  new AuthInfo(){ FullName = Utils.obj2str(data["budget_entryby"]) } },
                        { "is_disabled", !Utils.obj2bool(data["budget_isactive"]) },
                        { "disabled_date",  data["budget_disabledate"]},
                        { "disabled_by",  new AuthInfo(){ FullName = Utils.obj2str(data["budget_disabledby"]) } },
                        { "modified_date",  data["budget_lasteditdt"]},
                        { "modified_by",  new AuthInfo(){ FullName = Utils.obj2str(data["budget_lasteditby"]) } },
                        { "tprogrambudgetid",  null},
                        { "apprauthby",  data["budget_apprby"]},
                        { "apprauthdate",  data["budget_apprdt"]},
                        { "apprreqby",  null},
                        { "apprreqdate",  null},
                        { "apprverby",  null},
                        { "apprverdate",  null},
                    }
                );
            }

            return result;
        }

        public override MappedData additionalStaticData() {
            return null;
        }

        public override void runDependencies() {
            new MasterProdType(connections).run();
            new MasterProjectType(connections).run();
            new MasterShowInventoryCategory(connections).run();
            new MasterShowInventoryDepartment(connections).run();
            new MasterShowInventoryTimezone(connections).run();
            new MasterTvProgramType(connections).run();
            new TransactionProgramBudget(connections).run(false, 1925);
        }
    }
}
