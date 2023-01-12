using SurplusMigrator.Models;
using System;
using System.Linq;

namespace SurplusMigrator.Tasks {
    class MasterTaxCategory : _BaseTask {
        public MasterTaxCategory(DbConnection_[] connections) : base(connections) {
            sources = new TableInfo[] {
            };
            destinations = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().name == "surplus").FirstOrDefault(),
                    tableName = "master_tax_category",
                    columns = new string[] {
                        "taxcategoryid",
                        "name",
                        "created_date",
                        "created_by",
                        "is_disabled"
                    },
                    ids = new string[] { "taxcategoryid" }
                }
            };
        }

        protected override MappedData getStaticData() {
            MappedData result = new MappedData();

            result.addData(
                "master_tax_category",
                new RowData<ColumnName, object>() {
                    { "taxcategoryid", 1},
                    { "name", "No Tax"},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_tax_category",
                new RowData<ColumnName, object>() {
                    { "taxcategoryid", 2},
                    { "name", "PPh21"},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_tax_category",
                new RowData<ColumnName, object>() {
                    { "taxcategoryid", 3},
                    { "name", "PPh23"},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_tax_category",
                new RowData<ColumnName, object>() {
                    { "taxcategoryid", 4},
                    { "name", "PPh23 - Royalti"},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_tax_category",
                new RowData<ColumnName, object>() {
                    { "taxcategoryid", 7},
                    { "name", "PPh21-Hadiah/Kuis"},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_tax_category",
                new RowData<ColumnName, object>() {
                    { "taxcategoryid", 8},
                    { "name", "PPh 4(2) Final penghasilan tertentu"},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_tax_category",
                new RowData<ColumnName, object>() {
                    { "taxcategoryid", 9},
                    { "name", "PPh 4(2) Final Sewa Bangunan/Studio"},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_tax_category",
                new RowData<ColumnName, object>() {
                    { "taxcategoryid", 11},
                    { "name", "PPh26 - Royalti"},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_tax_category",
                new RowData<ColumnName, object>() {
                    { "taxcategoryid", 12},
                    { "name", "PPh26"},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );

            return result;
        }
    }
}
