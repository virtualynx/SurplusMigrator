using Microsoft.Data.SqlClient;
using Npgsql;
using SurplusMigrator.Libraries;
using SurplusMigrator.Models;
using SurplusMigrator.Tasks;
using System;
using System.Collections.Generic;
using System.Linq;

namespace SurplusMigrator.Tasks {
  class MasterShowInventoryDepartment : _BaseTask {
        public MasterShowInventoryDepartment(DbConnection_[] connections) : base(connections) {
            sources = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().dbname == "E_FRM").FirstOrDefault(),
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
                    connection = connections.Where(a => a.GetDbLoginInfo().dbname == "insosys").FirstOrDefault(),
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

        public override List<RowData<ColumnName, Data>> getSourceData(Table[] sourceTables, int batchSize = 5000) {
            return sourceTables.Where(a => a.tableName == "master_showinventorydepartment").FirstOrDefault().getDatas(batchSize);
        }

        public override MappedData mapData(List<RowData<ColumnName, Data>> inputs) {
            MappedData result = new MappedData();

            foreach(RowData<ColumnName, Data> data in inputs) {
                RowData<ColumnName, Data> insertRow = new RowData<ColumnName, Data>() {
                    { "showinventorydepartmentid",  data["showinventorydepartment_id"]},
                    { "name",  data["showinventorydepartment_name"]},
                    { "created_date",  data["showinventorydepartment_createddate"]},
                    { "created_by",  new AuthInfo(){ FullName = Utils.obj2str(data["showinventorydepartment_createdby"]) } },
                    { "is_disabled", Utils.obj2bool(data["showinventorydepartment_isdisabled"]) }
                };
                result.addData("master_show_inventory_department", insertRow);
            }

            return result;
        }

        public override MappedData additionalStaticData() {
            return null;
        }

        public override void runDependencies() {
        }
    }
}
