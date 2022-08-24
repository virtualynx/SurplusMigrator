using SurplusMigrator.Models;
using System;
using System.Collections.Generic;
using System.Linq;

namespace SurplusMigrator.Tasks {
    class MasterProgramBudgetContenttype : _BaseTask {
        public MasterProgramBudgetContenttype(DbConnection_[] connections) : base(connections) {
            sources = new TableInfo[] {};
            destinations = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().dbname == "insosys").FirstOrDefault(),
                    tableName = "master_program_budget_contenttype",
                    columns = new string[] {
                        "programbudgetcontenttypeid",
                        "name",
                        "created_date",
                        "created_by",
                        "is_disabled"
                    },
                    ids = new string[] { "programbudgetcontenttypeid" }
                }
            };
        }

        public override List<RowData<ColumnName, object>> getSourceData(Table[] sourceTables, int batchSize = defaultReadBatchSize) {
            return new List<RowData<string, object>>();
        }

        public override MappedData mapData(List<RowData<ColumnName, object>> inputs) {
            return new MappedData();
        }

        public override MappedData additionalStaticData() {
            MappedData result = new MappedData();

            result.addData(
                "master_program_budget_contenttype",
                new RowData<ColumnName, object>() {
                    { "programbudgetcontenttypeid",  1},
                    { "name",  "Program"},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_program_budget_contenttype",
                new RowData<ColumnName, object>() {
                    { "programbudgetcontenttypeid",  2},
                    { "name",  "Digital"},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_program_budget_contenttype",
                new RowData<ColumnName, object>() {
                    { "programbudgetcontenttypeid",  3},
                    { "name",  "Media Trans"},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_program_budget_contenttype",
                new RowData<ColumnName, object>() {
                    { "programbudgetcontenttypeid",  4},
                    { "name",  "News"},
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
