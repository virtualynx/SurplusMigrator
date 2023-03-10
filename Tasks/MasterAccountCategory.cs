using SurplusMigrator.Models;
using System;
using System.Linq;

namespace SurplusMigrator.Tasks {
    class MasterAccountCategory : _BaseTask {
        public MasterAccountCategory(DbConnection_[] connections) : base(connections) {
            sources = new TableInfo[] {};
            destinations = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().name == "surplus").FirstOrDefault(),
                    tablename = "master_account_category",
                    columns = new string[] {
                        "type",
                        "name",
                        "created_date",
                        "created_by",
                        "is_disabled"
                    },
                    ids = new string[] { "type", "name" }
                }
            };
        }

        protected override MappedData getStaticData() {
            MappedData result = new MappedData();

            result.addData(
                "master_account_category",
                new RowData<ColumnName, object>() {
                    { "type",  "AP"},
                    { "name",  "ACCOUNT STATEMENT AP"},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_account_category",
                new RowData<ColumnName, object>() {
                    { "type",  "AP"},
                    { "name",  "AGING AP"},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_account_category",
                new RowData<ColumnName, object>() {
                    { "type",  "AR"},
                    { "name",  "AGING AR"},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_account_category",
                new RowData<ColumnName, object>() {
                    { "type",  "AR"},
                    { "name",  "ACCOUNT STATEMENT AR"},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_account_category",
                new RowData<ColumnName, object>() {
                    { "type",  "UM"},
                    { "name",  "ACCOUNT STATEMENT UM"},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );

            return result;
        }
    }
}
