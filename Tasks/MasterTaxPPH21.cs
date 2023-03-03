using SurplusMigrator.Libraries;
using SurplusMigrator.Models;
using System;
using System.Collections.Generic;
using System.Linq;

namespace SurplusMigrator.Tasks {
    class MasterTaxPPH21 : _BaseTask {
        public MasterTaxPPH21(DbConnection_[] connections) : base(connections) {
            sources = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().name == "e_frm").FirstOrDefault(),
                    tableName = "master_pajak_pph21",
                    columns = new string[] {
                        "code_pajak",
                        "persen",
                        "minimal",
                        "maksimal",
                        "quota",
                        "persen_notnpwp"
                    },
                    ids = new string[] { "code_pajak" }
                }
            };
            destinations = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().name == "surplus").FirstOrDefault(),
                    tableName = "master_tax_pph21",
                    columns = new string[] {
                        "taxcategoryid",
                        "lowerlimit",
                        "upperlimit",
                        "quota",
                        "taxrate_npwp",
                        "taxrate_non_npwp",
                        "created_date",
                        "created_by",
                        "is_disabled"
                    },
                    ids = new string[] { "taxcategoryid", "taxrate_npwp", "taxrate_non_npwp" }
                }
            };
        }

        protected override List<RowData<ColumnName, object>> getSourceData(Table[] sourceTables, int batchSize = defaultReadBatchSize) {
            return sourceTables.Where(a => a.tableName == "master_pajak_pph21").FirstOrDefault().getData(batchSize);
        }

        protected override MappedData mapData(List<RowData<ColumnName, object>> inputs) {
            MappedData result = new MappedData();

            foreach(RowData<ColumnName, object> data in inputs) {
                result.addData(
                    "master_tax_pph21",
                    new RowData<ColumnName, object>() {
                        { "taxcategoryid", Utils.obj2int(data["code_pajak"])},
                        { "lowerlimit", Utils.obj2decimal(data["minimal"])},
                        { "upperlimit", Utils.obj2decimal(data["maksimal"])},
                        { "quota", Utils.obj2decimal(data["quota"])},
                        { "taxrate_npwp", Utils.obj2decimal(data["persen"])},
                        { "taxrate_non_npwp", Utils.obj2decimal(data["persen_notnpwp"])},
                        { "created_date",  DateTime.Now},
                        { "created_by",  DefaultValues.CREATED_BY},
                        { "is_disabled", false }
                    }
                );
            }

            return result;
        }

        protected override void runDependencies() {
            new MasterTaxCategory(connections).run();
        }
    }
}
