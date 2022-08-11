using Microsoft.Data.SqlClient;
using Npgsql;
using SurplusMigrator.Interfaces;
using SurplusMigrator.Tasks;
using System;
using System.Collections.Generic;
using System.Linq;

namespace SurplusMigrator.Models
{
  class MasterSource : _BaseTask {
        public MasterSource(DbConnection_[] connections) {
            sources = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().dbname == "E_FRM").FirstOrDefault(),
                    tableName = "master_source",
                    columns = new string[] {
                        "source_id",
                        "source_descr",
                        "type_id",
                    },
                    ids = new string[] { "source_id" }
                }
            };
            destinations = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().dbname == "insosys").FirstOrDefault(),
                    tableName = "master_source",
                    columns = new string[] {
                        "sourceid",
                        "description",
                        "transactiontypeid",
                        "created_date",
                        "created_by",
                        "is_disabled"
                    },
                    ids = new string[] { "sourceid" }
                }
            };
        }

        public override List<RowData<ColumnName, Data>> getSourceData(Table[] sourceTables) {
            return sourceTables.Where(a => a.tableName == "master_source").FirstOrDefault().getDatas();
        }

        public override MappedData mapData(List<RowData<ColumnName, Data>> inputs) {
            MappedData result = new MappedData();

            foreach(RowData<ColumnName, Data> data in inputs) {
                RowData<ColumnName, Data> insertRow = new RowData<ColumnName, Data>() {
                    { "sourceid",  data["source_id"]},
                    { "description",  data["source_descr"]},
                    { "transactiontypeid",  data["type_id"]},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                };
                result.addData("master_source", insertRow);
            }

            return result;
        }

        public override MappedData additionalStaticData() {
            return new MappedData();
        }
    }
}
