using Microsoft.Data.SqlClient;
using Npgsql;
using SurplusMigrator.Interfaces;
using SurplusMigrator.Tasks;
using System;
using System.Collections.Generic;
using System.Linq;

namespace SurplusMigrator.Models
{
  class MasterTransactionTypeGroup : _BaseTask {
        public MasterTransactionTypeGroup(DbConnection_[] connections) {
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

        public override List<RowData<ColumnName, Data>> getSourceData(Table[] sourceTables) {
            return new List<RowData<string, object>>();
        }

        public override MappedData mapData(List<RowData<ColumnName, Data>> inputs) {
            return new MappedData();
        }

        public override MappedData additionalStaticData() {
            MappedData result = new MappedData();
            
            result.addData(
                "master_transaction_type_group",
                new RowData<ColumnName, Data>() {
                    { "transactiontypegroupid",  1},
                    { "name",  "Request"},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_transaction_type_group",
                new RowData<ColumnName, Data>() {
                    { "transactiontypegroupid",  2},
                    { "name",  "Order"},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_transaction_type_group",
                new RowData<ColumnName, Data>() {
                    { "transactiontypegroupid",  3},
                    { "name",  "Good Receipt"},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_transaction_type_group",
                new RowData<ColumnName, Data>() {
                    { "transactiontypegroupid",  4},
                    { "name",  "Journal"},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_transaction_type_group",
                new RowData<ColumnName, Data>() {
                    { "transactiontypegroupid",  5},
                    { "name",  "Advance"},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_transaction_type_group",
                new RowData<ColumnName, Data>() {
                    { "transactiontypegroupid",  6},
                    { "name",  "Invoice"},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_transaction_type_group",
                new RowData<ColumnName, Data>() {
                    { "transactiontypegroupid",  7},
                    { "name",  "Budget"},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_transaction_type_group",
                new RowData<ColumnName, Data>() {
                    { "transactiontypegroupid",  8},
                    { "name",  "Inventory"},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );

            return result;
        }
    }
}
