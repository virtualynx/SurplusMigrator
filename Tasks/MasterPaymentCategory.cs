using SurplusMigrator.Models;
using System;
using System.Linq;

namespace SurplusMigrator.Tasks {
    class MasterPaymentCategory : _BaseTask {
        public MasterPaymentCategory(DbConnection_[] connections) : base(connections) {
            sources = new TableInfo[] {};
            destinations = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().name == "surplus").FirstOrDefault(),
                    tableName = "master_payment_category",
                    columns = new string[] {
                        "paymentcategoryid",
                        "name",
                        "created_date",
                        "created_by",
                        "is_disabled"
                    },
                    ids = new string[] { "paymentcategoryid" }
                }
            };
        }

        protected override MappedData getStaticData() {
            MappedData result = new MappedData();

            result.addData(
                "master_payment_category",
                new RowData<ColumnName, object>() {
                    { "paymentcategoryid",  1},
                    { "name",  "Pelunasan"},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_payment_category",
                new RowData<ColumnName, object>() {
                    { "paymentcategoryid",  2},
                    { "name",  "Termin"},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );

            return result;
        }

        protected override void runDependencies() {
            new MasterTransactionType(connections).run();
        }
    }
}
