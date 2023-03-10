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
                    connection = connections.Where(a => a.GetDbLoginInfo().name == "e_frm").FirstOrDefault(),
                    tablename = "master_showinventorycategory",
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
                    connection = connections.Where(a => a.GetDbLoginInfo().name == "surplus").FirstOrDefault(),
                    tablename = "master_show_inventory_category",
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

        protected override List<RowData<ColumnName, object>> getSourceData(Table[] sourceTables, int batchSize = defaultReadBatchSize) {
            return sourceTables.Where(a => a.tablename == "master_showinventorycategory").FirstOrDefault().getData(batchSize);
        }

        public override MappedData mapData(List<RowData<ColumnName, object>> inputs) {
            MappedData result = new MappedData();

            foreach(RowData<ColumnName, object> data in inputs) {
                RowData<ColumnName, object> insertRow = new RowData<ColumnName, object>() {
                    { "showinventorycategoryid",  data["showinventorycategory_id"]},
                    { "name",  data["showinventorycategory_name"]},
                    { "created_date",  data["showinventorycategory_createddate"]},
                    { "created_by", getAuthInfo(data["showinventorycategory_createdby"], true) },
                    { "is_disabled", Utils.obj2bool(data["showinventorycategory_isdisabled"]) }
                };
                result.addData("master_show_inventory_category", insertRow);
            }

            return result;
        }

        protected override MappedData getStaticData() {
            MappedData result = new MappedData();

            result.addData(
                "master_show_inventory_category",
                new RowData<ColumnName, object>() {
                    { "showinventorycategoryid",  0},
                    { "name",  "Empty - Migrations"},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY },
                    { "is_disabled", false }
                }
            );

            return result;
        }
    }
}
