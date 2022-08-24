using Microsoft.Data.SqlClient;
using Npgsql;
using SurplusMigrator.Libraries;
using SurplusMigrator.Models;
using SurplusMigrator.Tasks;
using System;
using System.Collections.Generic;
using System.Linq;

namespace SurplusMigrator.Tasks {
  class MasterAccount : _BaseTask {
        public MasterAccount(DbConnection_[] connections) : base(connections) {
            sources = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().dbname == "E_FRM").FirstOrDefault(),
                    tableName = "master_acc",
                    columns = new string[] {
                        "acc_id",
                        "acc_name",
                        "acc_nameshort",
                        "acc_descr",
                        "acc_isgroup",
                        "acc_parent",
                        "acc_path",
                        "acc_ismonetary",
                        "accsubgroup_id",
                        "acc_type",
                        "acc_isdisabled"
                    },
                    ids = new string[] { "acc_id" }
                }
            };
            destinations = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().dbname == "insosys").FirstOrDefault(),
                    tableName = "master_account",
                    columns = new string[] {
                        "accountid",
                        "name",
                        "nameshort",
                        "descr",
                        "isgroup",
                        "parent",
                        "path",
                        "ismonetary",
                        "accountsubgroupid",
                        "accounttypeid",
                        "created_date",
                        //"created_by",
                        "is_disabled"
                    },
                    ids = new string[] { "accountid" }
                }
            };
        }

        public override List<RowData<ColumnName, object>> getSourceData(Table[] sourceTables, int batchSize = defaultReadBatchSize) {
            return sourceTables.Where(a => a.tableName == "master_acc").FirstOrDefault().getDatas(batchSize);
        }

        public override MappedData mapData(List<RowData<ColumnName, object>> inputs) {
            MappedData result = new MappedData();

            foreach(RowData<ColumnName, object> data in inputs) {
                RowData<ColumnName, object> insertRow = new RowData<ColumnName, object>() {
                    { "accountid",  data["acc_id"]},
                    { "name",  data["acc_name"]},
                    { "nameshort",  data["acc_nameshort"]},
                    { "descr",  data["acc_descr"]},
                    { "isgroup",  Utils.obj2bool(data["acc_isgroup"])},
                    { "parent",  data["acc_parent"]},
                    { "path",  data["acc_path"]},
                    { "ismonetary", Utils.obj2bool(data["acc_ismonetary"])},
                    { "accountsubgroupid",  data["accsubgroup_id"]},
                    { "accounttypeid",  data["acc_type"]},
                    { "created_date",  DateTime.Now},
                    //{ "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", Utils.obj2bool(data["acc_isdisabled"]) }
                };
                result.addData("master_account", insertRow);
            }

            return result;
        }

        public override MappedData additionalStaticData() {
            MappedData result = new MappedData();

            result.addData(
                "master_account",
                new RowData<ColumnName, object>() {
                    { "accountid",  "1234358"},
                    { "name",  "Missing data 1234358"},
                    { "nameshort",  "Missing data 1234358"},
                    { "descr",  "Missing data referenced in master_budget_account(former-name master_projectacc) id 5012103550"},
                    { "isgroup",  false},
                    { "parent",  null},
                    { "path",  null},
                    { "ismonetary", false},
                    { "accountsubgroupid",  null},
                    { "accounttypeid",  null},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_account",
                new RowData<ColumnName, object>() {
                    { "accountid",  "7060044"},
                    { "name",  "Missing data 7060044"},
                    { "nameshort",  "Missing data 7060044"},
                    { "descr",  "Missing data referenced in master_budget_account(former-name master_projectacc) id 6070000030"},
                    { "isgroup",  false},
                    { "parent",  null},
                    { "path",  null},
                    { "ismonetary", false},
                    { "accountsubgroupid",  null},
                    { "accounttypeid",  null},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_account",
                new RowData<ColumnName, object>() {
                    { "accountid",  "15"},
                    { "name",  "Missing data 15"},
                    { "nameshort",  "Missing data 15"},
                    { "descr",  "Missing data referenced in transaction_budget_detail id: 163582, 154647, 160161, 157309"},
                    { "isgroup",  false},
                    { "parent",  null},
                    { "path",  null},
                    { "ismonetary", false},
                    { "accountsubgroupid",  null},
                    { "accounttypeid",  null},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );

            return result;
        }

        public override void runDependencies() {
            new MasterAccountReport(connections).run();
            new MasterAccountGroup(connections).run();
            new MasterAccountSubGroup(connections).run();
            new MasterAccountSubType(connections).run();
            new MasterAccountType(connections).run();
        }
    }
}
