using Microsoft.Data.SqlClient;
using Npgsql;
using SurplusMigrator.Libraries;
using SurplusMigrator.Models;
using SurplusMigrator.Tasks;
using System;
using System.Collections.Generic;
using System.Linq;

namespace SurplusMigrator.Tasks {
  class MasterTransactionTypeGroup : _BaseTask {
        public MasterTransactionTypeGroup(DbConnection_[] connections) : base(connections) {
            sources = new TableInfo[] {};
            destinations = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().dbname == "insosys").FirstOrDefault(),
                    tableName = "master_transaction_type_group",
                    columns = new string[] {
                        "transactiontypegroupid",
                        "name",
                        "created_date",
                        "created_by",
                        "is_disabled"
                    },
                    ids = new string[] { "transactiontypegroupid" }
                }
            };
        }

        protected override MappedData getStaticData() {
            MappedData result = new MappedData();
            
            result.addData(
                "master_transaction_type_group",
                new RowData<ColumnName, object>() {
                    { "transactiontypegroupid",  1},
                    { "name",  "Request"},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_transaction_type_group",
                new RowData<ColumnName, object>() {
                    { "transactiontypegroupid",  2},
                    { "name",  "Order"},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_transaction_type_group",
                new RowData<ColumnName, object>() {
                    { "transactiontypegroupid",  3},
                    { "name",  "Good Receipt"},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_transaction_type_group",
                new RowData<ColumnName, object>() {
                    { "transactiontypegroupid",  4},
                    { "name",  "Journal"},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_transaction_type_group",
                new RowData<ColumnName, object>() {
                    { "transactiontypegroupid",  5},
                    { "name",  "Advance"},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_transaction_type_group",
                new RowData<ColumnName, object>() {
                    { "transactiontypegroupid",  6},
                    { "name",  "Invoice"},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_transaction_type_group",
                new RowData<ColumnName, object>() {
                    { "transactiontypegroupid",  7},
                    { "name",  "Budget"},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_transaction_type_group",
                new RowData<ColumnName, object>() {
                    { "transactiontypegroupid",  8},
                    { "name",  "Inventory"},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_transaction_type_group",
                new RowData<ColumnName, object>() {
                    { "transactiontypegroupid",  9},
                    { "name",  "Settlement Content"},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );

            return result;
        }
    }
}
