using SurplusMigrator.Libraries;
using SurplusMigrator.Models;
using System;
using System.Collections.Generic;
using System.Linq;

namespace SurplusMigrator.Tasks {
    class MasterTaxType : _BaseTask {
        public MasterTaxType(DbConnection_[] connections) : base(connections) {
            sources = new TableInfo[] {
            };
            destinations = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().name == "surplus").FirstOrDefault(),
                    tableName = "master_tax_type",
                    columns = new string[] {
                        "taxtypeid",
                        "name",
                        "created_date",
                        "created_by",
                        "is_disabled",
                    },
                    ids = new string[] { "taxtypeid" }
                }
            };
        }

        protected override MappedData getStaticData() {
            MappedData result = new MappedData();

            result.addData(
                "master_tax_type",
                new RowData<ColumnName, object>() {
                    { "taxtypeid",  1},
                    { "name",  "Dipotong"},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_tax_type",
                new RowData<ColumnName, object>() {
                    { "taxtypeid",  2},
                    { "name",  "Gross Up"},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );

            return result;
        }
    }
}
