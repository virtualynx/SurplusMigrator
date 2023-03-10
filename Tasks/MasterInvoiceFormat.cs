using SurplusMigrator.Models;
using System;
using System.Linq;

namespace SurplusMigrator.Tasks {
    class MasterInvoiceFormat : _BaseTask {
        public MasterInvoiceFormat(DbConnection_[] connections) : base(connections) {
            sources = new TableInfo[] {};
            destinations = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().name == "surplus").FirstOrDefault(),
                    tablename = "master_invoice_format",
                    columns = new string[] {
                        "invoiceformatid",
                        "name",
                        "dw",
                        "code",
                        "created_date",
                        "created_by",
                        "is_disabled"
                    },
                    ids = new string[] { "invoiceformatid" }
                }
            };
        }

        protected override MappedData getStaticData() {
            MappedData result = new MappedData();

            //found in TransactionSalesOrder's reference
            result.addData(
                "master_invoice_format",
                new RowData<ColumnName, object>() {
                    { "invoiceformatid",  0},
                    { "name",  "Unknown"},
                    { "dw",  null},
                    { "code",  0},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_invoice_format",
                new RowData<ColumnName, object>() {
                    { "invoiceformatid",  1},
                    { "name",  "Inv-Satu"},
                    { "dw",  "sl_pm_invoice4_d"},
                    { "code",  1},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_invoice_format",
                new RowData<ColumnName, object>() {
                    { "invoiceformatid",  2},
                    { "name",  "Inv-Dua"},
                    { "dw",  "sl_pm_invoice2_d"},
                    { "code",  1},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_invoice_format",
                new RowData<ColumnName, object>() {
                    { "invoiceformatid",  3},
                    { "name",  "DN-Satu"},
                    { "dw",  "sl_pm_debitmemo_d"},
                    { "code",  2},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_invoice_format",
                new RowData<ColumnName, object>() {
                    { "invoiceformatid",  4},
                    { "name",  "CN-Satu"},
                    { "dw",  "sl_pm_creditmemo_d"},
                    { "code",  3},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_invoice_format",
                new RowData<ColumnName, object>() {
                    { "invoiceformatid",  5},
                    { "name",  "DR-Setengah"},
                    { "dw",  "sl_bill_del_rpt_d"},
                    { "code",  4},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_invoice_format",
                new RowData<ColumnName, object>() {
                    { "invoiceformatid",  6},
                    { "name",  "DR-Satu Lembar"},
                    { "dw",  "sl_bill_del_rpt2_d"},
                    { "code",  4},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_invoice_format",
                new RowData<ColumnName, object>() {
                    { "invoiceformatid",  7},
                    { "name",  "LG-Satu"},
                    { "dw",  null},
                    { "code",  5},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_invoice_format",
                new RowData<ColumnName, object>() {
                    { "invoiceformatid",  8},
                    { "name",  "LG-Dua"},
                    { "dw",  null},
                    { "code",  5},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );

            return result;
        }
    }
}
