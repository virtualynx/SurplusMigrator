using SurplusMigrator.Models;
using System;
using System.Linq;

namespace SurplusMigrator.Tasks {
    class MasterTransactionType : _BaseTask {
        public MasterTransactionType(DbConnection_[] connections) : base(connections) {
            sources = new TableInfo[] {};
            destinations = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().name == "surplus").FirstOrDefault(),
                    tablename = "master_transaction_type",
                    columns = new string[] {
                        "transactiontypeid",
                        "name",
                        "transactiontypegroupid",
                        "created_date",
                        "created_by",
                        "is_disabled"
                    },
                    ids = new string[] { "transactiontypeid" }
                }
            };
        }

        protected override MappedData getStaticData() {
            MappedData result = new MappedData();

            result.addData(
                "master_transaction_type",
                new RowData<ColumnName, object>() {
                    { "transactiontypeid",  "AP"},
                    { "name",  "Account Payable"},
                    { "transactiontypegroupid",  4},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_transaction_type",
                new RowData<ColumnName, object>() {
                    { "transactiontypeid",  "BE"},
                    { "name",  "BE"},
                    { "transactiontypegroupid",  1},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_transaction_type",
                new RowData<ColumnName, object>() {
                    { "transactiontypeid",  "BL"},
                    { "name",  "BL"},
                    { "transactiontypegroupid",  7},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_transaction_type",
                new RowData<ColumnName, object>() {
                    { "transactiontypeid",  "CD"},
                    { "name",  "Cancelation Digital"},
                    { "transactiontypegroupid",  9},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_transaction_type",
                new RowData<ColumnName, object>() {
                    { "transactiontypeid",  "CE"},
                    { "name",  "Cancelation Episode"},
                    { "transactiontypegroupid",  9},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_transaction_type",
                new RowData<ColumnName, object>() {
                    { "transactiontypeid",  "CN"},
                    { "name",  "CN"},
                    { "transactiontypegroupid",  1},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_transaction_type",
                new RowData<ColumnName, object>() {
                    { "transactiontypeid",  "CQ"},
                    { "name",  "CQ"},
                    { "transactiontypegroupid",  5},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_transaction_type",
                new RowData<ColumnName, object>() {
                    { "transactiontypeid",  "CS"},
                    { "name",  "CS"},
                    { "transactiontypegroupid",  1},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_transaction_type",
                new RowData<ColumnName, object>() {
                    { "transactiontypeid",  "DN"},
                    { "name",  "DN"},
                    { "transactiontypegroupid",  1},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_transaction_type",
                new RowData<ColumnName, object>() {
                    { "transactiontypeid",  "EO"},
                    { "name",  "Editing Order"},
                    { "transactiontypegroupid",  2},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_transaction_type",
                new RowData<ColumnName, object>() {
                    { "transactiontypeid",  "GR"},
                    { "name",  "Goods Receipt"},
                    { "transactiontypegroupid",  2},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_transaction_type",
                new RowData<ColumnName, object>() {
                    { "transactiontypeid",  "JV"},
                    { "name",  "JV"},
                    { "transactiontypegroupid",  1},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_transaction_type",
                new RowData<ColumnName, object>() {
                    { "transactiontypeid",  "MO"},
                    { "name",  "Maintenance Order"},
                    { "transactiontypegroupid",  2},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_transaction_type",
                new RowData<ColumnName, object>() {
                    { "transactiontypeid",  "NO"},
                    { "name",  "General Order"},
                    { "transactiontypegroupid",  2},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_transaction_type",
                new RowData<ColumnName, object>() {
                    { "transactiontypeid",  "OC"},
                    { "name",  "Jurnal OC"},
                    { "transactiontypegroupid",  1},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_transaction_type",
                new RowData<ColumnName, object>() {
                    { "transactiontypeid",  "OR"},
                    { "name",  "OR"},
                    { "transactiontypegroupid",  1},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_transaction_type",
                new RowData<ColumnName, object>() {
                    { "transactiontypeid",  "OT"},
                    { "name",  "Official Travel"},
                    { "transactiontypegroupid",  10},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_transaction_type",
                new RowData<ColumnName, object>() {
                    { "transactiontypeid",  "PA"},
                    { "name",  "PA"},
                    { "transactiontypegroupid",  1},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_transaction_type",
                new RowData<ColumnName, object>() {
                    { "transactiontypeid",  "PO"},
                    { "name",  "Purchase Order"},
                    { "transactiontypegroupid",  2},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_transaction_type",
                new RowData<ColumnName, object>() {
                    { "transactiontypeid",  "PV"},
                    { "name",  "PV"},
                    { "transactiontypegroupid",  1},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_transaction_type",
                new RowData<ColumnName, object>() {
                    { "transactiontypeid",  "RI"},
                    { "name",  "Receive Invoice"},
                    { "transactiontypegroupid",  6},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_transaction_type",
                new RowData<ColumnName, object>() {
                    { "transactiontypeid",  "RO"},
                    { "name",  "Rental Order"},
                    { "transactiontypegroupid",  2},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_transaction_type",
                new RowData<ColumnName, object>() {
                    { "transactiontypeid",  "RT"},
                    { "name",  "Request Transaction"},
                    { "transactiontypegroupid",  1},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_transaction_type",
                new RowData<ColumnName, object>() {
                    { "transactiontypeid",  "RV"},
                    { "name",  "RV"},
                    { "transactiontypegroupid",  1},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_transaction_type",
                new RowData<ColumnName, object>() {
                    { "transactiontypeid",  "SA"},
                    { "name",  "Jurnal SA"},
                    { "transactiontypegroupid",  6},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_transaction_type",
                new RowData<ColumnName, object>() {
                    { "transactiontypeid",  "SD"},
                    { "name",  "Settlement Digital"},
                    { "transactiontypegroupid",  9},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_transaction_type",
                new RowData<ColumnName, object>() {
                    { "transactiontypeid",  "SE"},
                    { "name",  "Settlement Episode"},
                    { "transactiontypegroupid",  9},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_transaction_type",
                new RowData<ColumnName, object>() {
                    { "transactiontypeid",  "SJ"},
                    { "name",  "SJ"},
                    { "transactiontypegroupid",  1},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_transaction_type",
                new RowData<ColumnName, object>() {
                    { "transactiontypeid",  "SO"},
                    { "name",  "Sales Order"},
                    { "transactiontypegroupid",  6},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_transaction_type",
                new RowData<ColumnName, object>() {
                    { "transactiontypeid",  "ST"},
                    { "name",  "ST"},
                    { "transactiontypegroupid",  1},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_transaction_type",
                new RowData<ColumnName, object>() {
                    { "transactiontypeid",  "SVT"},
                    { "name",  "SVT"},
                    { "transactiontypegroupid",  7},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_transaction_type",
                new RowData<ColumnName, object>() {
                    { "transactiontypeid",  "TR"},
                    { "name",  "TR"},
                    { "transactiontypegroupid",  1},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_transaction_type",
                new RowData<ColumnName, object>() {
                    { "transactiontypeid",  "TU"},
                    { "name",  "Talent Unit"},
                    { "transactiontypegroupid",  1},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_transaction_type",
                new RowData<ColumnName, object>() {
                    { "transactiontypeid",  "VQ"},
                    { "name",  "Advance Request"},
                    { "transactiontypegroupid",  5},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );

            return result;
        }

        protected override void runDependencies() {
            new MasterTransactionTypeGroup(connections).run();
        }
    }
}
