using Microsoft.Data.SqlClient;
using SurplusMigrator.Libraries;
using SurplusMigrator.Models;
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

        protected override List<RowData<ColumnName, object>> getSourceData(Table[] sourceTables, int batchSize = defaultReadBatchSize) {
            return sourceTables.Where(a => a.tableName == "master_currency").FirstOrDefault().getDatas(batchSize);
        }

        protected override MappedData mapData(List<RowData<ColumnName, object>> inputs) {
            MappedData result = new MappedData();

            foreach(RowData<ColumnName, object> data in inputs) {
                RowData<ColumnName, object> insertRow = new RowData<ColumnName, object>() {
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

        protected override MappedData getStaticData() {
            MappedData result = new MappedData();

            result.addData(
                "master_currency",
                new RowData<ColumnName, object>() {
                    { "currencyid",  0},
                    { "shortname",  "UNKWN"},
                    { "name",  "Unknown"},
                    { "country",  "Unknown"},
                    { "is_disabled",  false},
                }
            );

            SqlConnection conn = (SqlConnection)connections.Where(a => a.GetDbLoginInfo().dbname == "E_FRM").FirstOrDefault().GetDbConnection();
            SqlCommand command = new SqlCommand("select max(currency_id) from [dbo].[master_currency]", conn);
            long largestCurrencyId = Convert.ToInt64(command.ExecuteScalar());

            result.addData(
                "master_currency",
                new RowData<ColumnName, object>() {
                    { "currencyid",  ++largestCurrencyId},
                    { "shortname",  "CNH"},
                    { "name",  "Offshore CNY"},
                    { "country",  "Republik Rakyat Cina"},
                    { "is_disabled",  false},
                }
            );
            result.addData(
                "master_currency",
                new RowData<ColumnName, object>() {
                    { "currencyid",  ++largestCurrencyId},
                    { "shortname",  "KWD"},
                    { "name",  "Kuwaiti Dinar"},
                    { "country",  "Kuwait"},
                    { "is_disabled",  false},
                }
            );
            result.addData(
                "master_currency",
                new RowData<ColumnName, object>() {
                    { "currencyid",  ++largestCurrencyId},
                    { "shortname",  "LAK"},
                    { "name",  "Laotian Kip"},
                    { "country",  "Laos"},
                    { "is_disabled",  false},
                }
            );
            result.addData(
                "master_currency",
                new RowData<ColumnName, object>() {
                    { "currencyid",  ++largestCurrencyId},
                    { "shortname",  "SAR"},
                    { "name",  "Saudi Riyal"},
                    { "country",  "Saudi Arabia"},
                    { "is_disabled",  false},
                }
            );

            return result;
        }
    }
}
