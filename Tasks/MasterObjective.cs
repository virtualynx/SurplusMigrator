using SurplusMigrator.Models;
using System;
using System.Linq;

namespace SurplusMigrator.Tasks {
    class MasterObjective : _BaseTask {
        public MasterObjective(DbConnection_[] connections) : base(connections) {
            sources = new TableInfo[] {};
            destinations = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().name == "surplus").FirstOrDefault(),
                    tablename = "master_objective",
                    columns = new string[] {
                        "objectiveid",
                        "name",
                        "created_date",
                        "created_by",
                        "is_disabled"
                    },
                    ids = new string[] { "objectiveid" }
                }
            };
        }

        protected override MappedData getStaticData() {
            MappedData result = new MappedData();

            result.addData(
                "master_objective",
                new RowData<ColumnName, object>() {
                    { "objectiveid",  "I"},
                    { "name",  "Inventory"},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_objective",
                new RowData<ColumnName, object>() {
                    { "objectiveid",  "M"},
                    { "name",  "Maintenance"},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_objective",
                new RowData<ColumnName, object>() {
                    { "objectiveid",  "N"},
                    { "name",  "General Service"},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_objective",
                new RowData<ColumnName, object>() {
                    { "objectiveid",  "P"},
                    { "name",  "Purchase"},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_objective",
                new RowData<ColumnName, object>() {
                    { "objectiveid",  "R"},
                    { "name",  "Rental"},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_objective",
                new RowData<ColumnName, object>() {
                    { "objectiveid",  "E"},
                    { "name",  "Editing"},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_objective",
                new RowData<ColumnName, object>() {
                    { "objectiveid",  "C"},
                    { "name",  "Canceled"},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );

            return result;
        }
    }
}
