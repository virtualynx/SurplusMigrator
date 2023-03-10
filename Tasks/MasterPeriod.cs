using Microsoft.Data.SqlClient;
using Npgsql;
using SurplusMigrator.Libraries;
using SurplusMigrator.Models;
using SurplusMigrator.Tasks;
using System;
using System.Collections.Generic;
using System.Linq;

namespace SurplusMigrator.Tasks {
  class MasterPeriod : _BaseTask {
        public MasterPeriod(DbConnection_[] connections) : base(connections) {
            sources = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().name == "e_frm").FirstOrDefault(),
                    tablename = "master_periode",
                    columns = new string[] {
                        "periode_id",
                        "periode_name",
                        "periode_datestart",
                        "periode_dateend",
                        "periode_isclosed",
                        "periode_createby",
                        "periode_createdate",
                        "periode_allowsaldoawalentry",
                    },
                    ids = new string[] { "periode_id" }
                }
            };
            destinations = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().name == "surplus").FirstOrDefault(),
                    tablename = "master_period",
                    columns = new string[] {
                        "periodid",
                        "name",
                        "datestart",
                        "dateend",
                        "isclosed",
                        "allowsaldoawalentry",
                        //"closeddate",
                        //"closedby",
                        "created_date",
                        "created_by",
                        "is_disabled"
                    },
                    ids = new string[] { "periodid" }
                }
            };
        }

        protected override List<RowData<ColumnName, object>> getSourceData(Table[] sourceTables, int batchSize = defaultReadBatchSize) {
            return sourceTables.Where(a => a.tablename == "master_periode").FirstOrDefault().getData(batchSize);
        }

        public override MappedData mapData(List<RowData<ColumnName, object>> inputs) {
            MappedData result = new MappedData();

            foreach(RowData<ColumnName, object> data in inputs) {
                RowData<ColumnName, object> insertRow = new RowData<ColumnName, object>() {
                    { "periodid",  data["periode_id"]},
                    { "name",  data["periode_name"]},
                    { "datestart",  data["periode_datestart"]},
                    { "dateend",  data["periode_dateend"]},
                    { "isclosed",  Utils.obj2bool(data["periode_isclosed"])},
                    { "allowsaldoawalentry",  Utils.obj2bool(data["periode_allowsaldoawalentry"])},
                    { "created_date",  data["periode_createdate"]},
                    { "created_by",  getAuthInfo(data["periode_createby"], true)},
                    { "is_disabled", false }
                };
                result.addData("master_period", insertRow);
            }

            return result;
        }
    }
}
