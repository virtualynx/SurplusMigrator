using Microsoft.Data.SqlClient;
using Npgsql;
using SurplusMigrator.Libraries;
using SurplusMigrator.Models;
using SurplusMigrator.Tasks;
using System;
using System.Collections.Generic;
using System.Linq;

namespace SurplusMigrator.Tasks {
  class MasterTaxRange : _BaseTask {
        public MasterTaxRange(DbConnection_[] connections) : base(connections) {
            sources = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().name == "finapptv7").FirstOrDefault(),
                    tableName = "ta_dump",
                    columns = new string[] {
                        "dt_start",
                        "id_start",
                        "id_end",
                        "prefix",
                        "flag"
                    },
                    ids = new string[] { "id_start", "id_end" }
                }
            };
            destinations = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().name == "surplus").FirstOrDefault(),
                    tableName = "master_tax_range",
                    columns = new string[] {
                        "prefix",
                        "start",
                        "end",
                        "date",
                        "currently_used",
                        "created_date",
                        "created_by",
                        "is_disabled"
                    },
                    ids = new string[] { "start", "end" }
                }
            };
        }

        protected override List<RowData<ColumnName, object>> getSourceData(Table[] sourceTables, int batchSize = defaultReadBatchSize) {
            return sourceTables.Where(a => a.tableName == "ta_dump").FirstOrDefault().getDatas(batchSize);
        }

        protected override MappedData mapData(List<RowData<ColumnName, object>> inputs) {
            MappedData result = new MappedData();

            foreach(RowData<ColumnName, object> data in inputs) {
                result.addData(
                    "master_tax_range",
                    new RowData<ColumnName, object>() {
                        { "prefix",  data["prefix"]},
                        { "start",  data["id_start"]},
                        { "end",  data["id_end"]},
                        { "date",  data["dt_start"]},
                        { "currently_used", Utils.obj2bool(data["flag"])},
                        { "created_date",  DateTime.Now},
                        { "created_by",  DefaultValues.CREATED_BY},
                        { "is_disabled", false}
                    }
                );
            }

            return result;
        }
    }
}
