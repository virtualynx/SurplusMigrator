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
                    connection = connections.Where(a => a.GetDbLoginInfo().name == "surplus").FirstOrDefault(),
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

        protected override MappedData getStaticData() {
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

            return result;
        }
    }
}
