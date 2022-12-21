using Microsoft.Data.SqlClient;
using Npgsql;
using SurplusMigrator.Libraries;
using SurplusMigrator.Models;
using SurplusMigrator.Tasks;
using System;
using System.Collections.Generic;
using System.Linq;

namespace SurplusMigrator.Tasks {
  class MasterModule : _BaseTask {
        public MasterModule(DbConnection_[] connections) : base(connections) {
            DbConnection_ surplusDev = new DbConnection_(new DbLoginInfo() { 
                host = "172.16.123.121",
                port = 5432,
                username = "postgres",
                password = "initrans7",
                dbname = "insosys",
                schema = "dev",
                type = "POSTGRESQL"
            });

            sources = new TableInfo[] {
                new TableInfo() {
                    connection = surplusDev,
                    tableName = "master_module",
                    columns = new string[] {
                        "moduleid",
                        "name",
                        "endpoint",
                        "type",
                        "created_date",
                        "created_by",
                        "is_disabled"
                    },
                    ids = new string[] { "moduleid" }
                }
            };
            destinations = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().dbname == "insosys").FirstOrDefault(),
                    tableName = "master_module",
                    columns = new string[] {
                        "moduleid",
                        "name",
                        "endpoint",
                        "type",
                        "created_date",
                        "created_by",
                        "is_disabled"
                    },
                    ids = new string[] { "moduleid" }
                }
            };
        }

        protected override List<RowData<ColumnName, object>> getSourceData(Table[] sourceTables, int batchSize = defaultReadBatchSize) {
            return sourceTables.Where(a => a.tableName == "master_module").FirstOrDefault().getDatas(batchSize);
        }

        protected override MappedData mapData(List<RowData<ColumnName, object>> inputs) {
            MappedData result = new MappedData();

            foreach(RowData<ColumnName, object> data in inputs) {
                RowData<ColumnName, object> insertRow = new RowData<ColumnName, object>() {
                    { "moduleid",  data["moduleid"]},
                    { "name",  data["name"]},
                    { "endpoint",  data["endpoint"]},
                    { "type",  data["type"]},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", data["is_disabled"] }
                };
                result.addData("master_module", insertRow);
            }

            return result;
        }
    }
}
