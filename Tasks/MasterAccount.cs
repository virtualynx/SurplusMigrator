using Microsoft.Data.SqlClient;
using Npgsql;
using SurplusMigrator.Interfaces;
using SurplusMigrator.Models;
using SurplusMigrator.Tasks;
using System;
using System.Collections.Generic;
using System.Linq;

namespace SurplusMigrator.Tasks {
  class MasterAccount : _BaseTask {
        public MasterAccount(DbConnection_[] connections) {
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

        public override List<RowData<ColumnName, Data>> getSourceData(Table[] sourceTables, int batchSize = 5000) {
            return sourceTables.Where(a => a.tableName == "master_acc").FirstOrDefault().getDatas(batchSize);
        }

        public override MappedData mapData(List<RowData<ColumnName, Data>> inputs) {
            MappedData result = new MappedData();

            foreach(RowData<ColumnName, Data> data in inputs) {
                RowData<ColumnName, Data> insertRow = new RowData<ColumnName, Data>() {
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
            return new MappedData();
        }
    }
}
