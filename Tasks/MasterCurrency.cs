using Microsoft.Data.SqlClient;
using Npgsql;
using SurplusMigrator.Libraries;
using SurplusMigrator.Models;
using SurplusMigrator.Tasks;
using System;
using System.Collections.Generic;
using System.Linq;

namespace SurplusMigrator.Tasks {
  class MasterCurrency : _BaseTask {
        public MasterCurrency(DbConnection_[] connections) : base(connections) {
            sources = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().dbname == "E_FRM").FirstOrDefault(),
                    tableName = "master_currency",
                    columns = new string[] {
                        "currency_id",
                        "currency_shortname",
                        "currency_name",
                        "currency_country",
                        "currency_active",
                    },
                    ids = new string[] { "currency_id" }
                }
            };
            destinations = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().dbname == "insosys").FirstOrDefault(),
                    tableName = "master_currency",
                    columns = new string[] {
                        "currencyid",
                        "shortname",
                        "name",
                        "country",
                        "is_disabled",
                    },
                    ids = new string[] { "currencyid" }
                }
            };
        }

        public override List<RowData<ColumnName, Data>> getSourceData(Table[] sourceTables, int batchSize = 5000) {
            return sourceTables.Where(a => a.tableName == "master_currency").FirstOrDefault().getDatas(batchSize);
        }

        public override MappedData mapData(List<RowData<ColumnName, Data>> inputs) {
            MappedData result = new MappedData();

            foreach(RowData<ColumnName, Data> data in inputs) {
                RowData<ColumnName, Data> insertRow = new RowData<ColumnName, Data>() {
                    { "currencyid",  data["currency_id"]},
                    { "shortname",  data["currency_shortname"]},
                    { "name",  data["currency_name"]},
                    { "country",  data["currency_country"]},
                    { "is_disabled",  !Utils.obj2bool(data["currency_active"])},
                };
                result.addData("master_currency", insertRow);
            }

            return result;
        }

        public override MappedData additionalStaticData() {
            MappedData result = new MappedData();

            result.addData(
                "master_currency",
                new RowData<ColumnName, Data>() {
                    { "currencyid",  0},
                    { "shortname",  "UNKWN"},
                    { "name",  "Unknown"},
                    { "country",  "Unknown"},
                    { "is_disabled",  false},
                }
            );

            return result;
        }

        public override void runDependencies() {
        }
    }
}
