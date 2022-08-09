using Microsoft.Data.SqlClient;
using Npgsql;
using SurplusMigrator.Interfaces;
using SurplusMigrator.Tasks;
using System;
using System.Collections.Generic;
using System.Linq;

namespace SurplusMigrator.Models {
    class MasterAccountGroup : _BaseTask {
        public MasterAccountGroup(DbConnection_[] connections) {
            sources = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().dbname == "E_FRM").FirstOrDefault(),
                    tableName = "master_accgroup",
                    columns = new string[] { "accgroup_id", "accgroup_name", "accgroup_position", "accrpt_id" },
                    ids = new string[] { "accgroup_id" }
                }
            };
            destinations = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().dbname == "insosys").FirstOrDefault(),
                    tableName = "master_account_group",
                    columns = new string[] {
                        "accountgroupid",
                        "name",
                        "position",
                        "accountreporttypeid",
                        "created_date",
                        "created_by",
                        "is_disabled"
                    },
                    ids = new string[] { "accountgroupid" }
                }
            };
        }

        public override List<RowData<ColumnName, Data>> getSourceData(Table[] sourceTables) {
            return sourceTables.Where(a => a.tableName == "master_accgroup").FirstOrDefault().getDatas();
        }

        public override MappedData mapData(List<RowData<ColumnName, Data>> inputs) {
            MappedData result = new MappedData();

            foreach(RowData<ColumnName, Data> data in inputs) {
                RowData<ColumnName, Data> insertRow = new RowData<ColumnName, Data>() {
                    { "accountgroupid",  data["accgroup_id"]},
                    { "name",  data["accgroup_name"]},
                    { "position",  data["accgroup_position"]},
                    { "accountreporttypeid",  data["accrpt_id"]},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                };
                result.addData("master_account_group", insertRow);
            }

            return result;
        }

        public override MappedData additionalStaticData() {
            MappedData result = new MappedData();

            result.addData(
                "master_account_group",
                new RowData<ColumnName, Data>() {
                    { "accountsubgroupid",  0},
                    { "name",  "Unknown"},
                    { "position",  null},
                    { "accountreporttypeid",  data["accrpt_id"]},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );

            return result;
        }
    }
}
