using Microsoft.Data.SqlClient;
using Npgsql;
using SurplusMigrator.Libraries;
using SurplusMigrator.Models;
using SurplusMigrator.Tasks;
using System;
using System.Collections.Generic;
using System.Linq;

namespace SurplusMigrator.Tasks {
  class MasterShowInventoryCategory : _BaseTask {
        public MasterShowInventoryCategory(DbConnection_[] connections) : base(connections) {
            sources = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().dbname == "E_FRM").FirstOrDefault(),
                    tableName = "master_showinventorycategory",
                    columns = new string[] {
                        "showinventorycategory_id",
                        "showinventorycategory_name",
                        "showinventorycategory_isdisabled",
                        "showinventorycategory_createdby",
                        "showinventorycategory_createddate",
                    },
                    ids = new string[] { "showinventorycategory_id" }
                }
            };
            destinations = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().dbname == "insosys").FirstOrDefault(),
                    tableName = "master_show_inventory_category",
                    columns = new string[] {
                        "showinventorycategoryid",
                        "name",
                        "created_date",
                        "created_by",
                        "is_disabled"
                    },
                    ids = new string[] { "showinventorycategoryid" }
                }
            };
        }

        public override List<RowData<ColumnName, Data>> getSourceData(Table[] sourceTables, int batchSize = 5000) {
            return sourceTables.Where(a => a.tableName == "master_showinventorycategory").FirstOrDefault().getDatas(batchSize);
        }

        public override MappedData mapData(List<RowData<ColumnName, Data>> inputs) {
            MappedData result = new MappedData();

            foreach(RowData<ColumnName, Data> data in inputs) {
                RowData<ColumnName, Data> insertRow = new RowData<ColumnName, Data>() {
                    { "showinventorycategoryid",  data["showinventorycategory_id"]},
                    { "name",  data["showinventorycategory_name"]},
                    { "created_date",  data["showinventorycategory_createddate"]},
                    { "created_by",  new AuthInfo(){ FullName = Utils.obj2str(data["showinventorycategory_createdby"]) } },
                    { "is_disabled", Utils.obj2bool(data["showinventorycategory_isdisabled"]) }
                };
                result.addData("master_show_inventory_category", insertRow);
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
