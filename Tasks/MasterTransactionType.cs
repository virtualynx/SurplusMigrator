using Microsoft.Data.SqlClient;
using Npgsql;
using SurplusMigrator.Interfaces;
using SurplusMigrator.Models;
using SurplusMigrator.Tasks;
using System;
using System.Collections.Generic;
using System.Linq;

namespace SurplusMigrator.Tasks {
  class MasterTransactionType : _BaseTask {
        public MasterTransactionType(DbConnection_[] connections) : base(connections) {
            sources = new TableInfo[] {};
            destinations = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().dbname == "insosys").FirstOrDefault(),
                    tableName = "master_transaction_type",
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

        public override List<RowData<ColumnName, Data>> getSourceData(Table[] sourceTables, int batchSize = 5000) {
            return new List<RowData<ColumnName, Data>>();
        }

        public override MappedData mapData(List<RowData<ColumnName, Data>> inputs) {
            return new MappedData(); ;
        }

        public override MappedData additionalStaticData() {
            MappedData result = new MappedData();

            result.addData(
                "master_transaction_type",
                new RowData<ColumnName, Data>() {
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
                new RowData<ColumnName, Data>() {
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
                new RowData<ColumnName, Data>() {
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
                new RowData<ColumnName, Data>() {
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
                new RowData<ColumnName, Data>() {
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
                new RowData<ColumnName, Data>() {
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
                new RowData<ColumnName, Data>() {
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
                new RowData<ColumnName, Data>() {
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
                new RowData<ColumnName, Data>() {
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
                new RowData<ColumnName, Data>() {
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
                new RowData<ColumnName, Data>() {
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
                new RowData<ColumnName, Data>() {
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
                new RowData<ColumnName, Data>() {
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
                new RowData<ColumnName, Data>() {
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
                new RowData<ColumnName, Data>() {
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
                new RowData<ColumnName, Data>() {
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
                new RowData<ColumnName, Data>() {
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
                new RowData<ColumnName, Data>() {
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
                new RowData<ColumnName, Data>() {
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
                new RowData<ColumnName, Data>() {
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
                new RowData<ColumnName, Data>() {
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
                new RowData<ColumnName, Data>() {
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
                new RowData<ColumnName, Data>() {
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
                new RowData<ColumnName, Data>() {
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
                new RowData<ColumnName, Data>() {
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
                new RowData<ColumnName, Data>() {
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
                new RowData<ColumnName, Data>() {
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
                new RowData<ColumnName, Data>() {
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
                new RowData<ColumnName, Data>() {
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
                new RowData<ColumnName, Data>() {
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
                new RowData<ColumnName, Data>() {
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
                new RowData<ColumnName, Data>() {
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
                new RowData<ColumnName, Data>() {
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

        public override void runDependencies() {
        }
    }
}
