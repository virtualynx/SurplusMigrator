using SurplusMigrator.Libraries;
using SurplusMigrator.Models;
using System;
using System.Collections.Generic;
using System.Linq;

namespace SurplusMigrator.Tasks {
    class MasterShowInventoryDepartment : _BaseTask {
        public MasterShowInventoryDepartment(DbConnection_[] connections) : base(connections) {
            sources = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().name == "e_frm").FirstOrDefault(),
                    tableName = "master_showinventorydepartment",
                    columns = new string[] {
                        "showinventorydepartment_id",
                        "showinventorydepartment_name",
                        "showinventorydepartment_isdisabled",
                        "showinventorydepartment_createdby",
                        "showinventorydepartment_createddate",
                    },
                    ids = new string[] { "showinventorydepartment_id" }
                }
            };
            destinations = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().name == "surplus").FirstOrDefault(),
                    tableName = "master_show_inventory_department",
                    columns = new string[] {
                        "showinventorydepartmentid",
                        "name",
                        "created_date",
                        "created_by",
                        "is_disabled"
                    },
                    ids = new string[] { "showinventorydepartmentid" }
                }
            };
        }

        protected override List<RowData<ColumnName, object>> getSourceData(Table[] sourceTables, int batchSize = defaultReadBatchSize) {
            return sourceTables.Where(a => a.tableName == "master_showinventorydepartment").FirstOrDefault().getData(batchSize);
        }

        protected override MappedData mapData(List<RowData<ColumnName, object>> inputs) {
            MappedData result = new MappedData();

            foreach(RowData<ColumnName, object> data in inputs) {
                RowData<ColumnName, object> insertRow = new RowData<ColumnName, object>() {
                    { "showinventorydepartmentid",  data["showinventorydepartment_id"]},
                    { "name",  data["showinventorydepartment_name"]},
                    { "created_date",  data["showinventorydepartment_createddate"]},
                    { "created_by", getAuthInfo(data["showinventorydepartment_createdby"], true) },
                    { "is_disabled", Utils.obj2bool(data["showinventorydepartment_isdisabled"]) }
                };
                result.addData("master_show_inventory_department", insertRow);
            }

            return result;
        }

        protected override MappedData getStaticData() {
            MappedData result = new MappedData();

            result.addData(
                "master_show_inventory_department",
                new RowData<ColumnName, object>() {
                    { "showinventorydepartmentid",  0},
                    { "name",  "Empty - Migrations"},
                    { "created_date",  DateTime.Now},
                    { "created_by", DefaultValues.CREATED_BY },
                    { "is_disabled", false }
                }
            );

            return result;
        }
    }
}
