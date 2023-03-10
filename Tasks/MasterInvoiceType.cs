using SurplusMigrator.Models;
using System;
using System.Linq;

namespace SurplusMigrator.Tasks {
    class MasterInvoiceType : _BaseTask {
        public MasterInvoiceType(DbConnection_[] connections) : base(connections) {
            sources = new TableInfo[] {};
            destinations = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().name == "surplus").FirstOrDefault(),
                    tablename = "master_invoice_type",
                    columns = new string[] {
                        "invoicetypeid",
                        "name",
                        "created_date",
                        "created_by",
                        "is_disabled"
                    },
                    ids = new string[] { "invoicetypeid" }
                }
            };
        }

        protected override MappedData getStaticData() {
            MappedData result = new MappedData();

            result.addData(
                "master_invoice_type",
                new RowData<ColumnName, object>() {
                    { "invoicetypeid",  0},
                    { "name",  "Unknown"},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_invoice_type",
                new RowData<ColumnName, object>() {
                    { "invoicetypeid",  1},
                    { "name",  "Bill"},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_invoice_type",
                new RowData<ColumnName, object>() {
                    { "invoicetypeid",  2},
                    { "name",  "Comp"},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_invoice_type",
                new RowData<ColumnName, object>() {
                    { "invoicetypeid",  3},
                    { "name",  "Billed (-DN)"},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );

            return result;
        }
    }
}
