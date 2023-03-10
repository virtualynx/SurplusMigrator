using Microsoft.Data.SqlClient;
using Npgsql;
using SurplusMigrator.Libraries;
using SurplusMigrator.Models;
using SurplusMigrator.Tasks;
using System;
using System.Collections.Generic;
using System.Linq;

namespace SurplusMigrator.Tasks {
  class MasterProgramBudgetType : _BaseTask {
        public MasterProgramBudgetType(DbConnection_[] connections) : base(connections) {
            sources = new TableInfo[] {};
            destinations = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().name == "surplus").FirstOrDefault(),
                    tablename = "master_program_budget_type",
                    columns = new string[] {
                        "programbudgettypeid",
                        "name",
                        "isapprovedbyprogramming",
                        "isreported",
                        "created_date",
                        "created_by",
                        "is_disabled"
                    },
                    ids = new string[] { "programbudgettypeid" }
                }
            };
        }

        protected override MappedData getStaticData() {
            MappedData result = new MappedData();

            result.addData(
                "master_program_budget_type",
                new RowData<ColumnName, object>() {
                    { "programbudgettypeid",  0},
                    { "name",  "NON PILOT"},
                    { "isapprovedbyprogramming",  true},
                    { "isreported",  true},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_program_budget_type",
                new RowData<ColumnName, object>() {
                    { "programbudgettypeid",  1},
                    { "name",  "PILOT"},
                    { "isapprovedbyprogramming",  true},
                    { "isreported",  true},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_program_budget_type",
                new RowData<ColumnName, object>() {
                    { "programbudgettypeid",  2},
                    { "name",  "NON EPISODE"},
                    { "isapprovedbyprogramming",  true},
                    { "isreported",  true},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_program_budget_type",
                new RowData<ColumnName, object>() {
                    { "programbudgettypeid",  3},
                    { "name",  "REPACKAGE"},
                    { "isapprovedbyprogramming",  true},
                    { "isreported",  true},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_program_budget_type",
                new RowData<ColumnName, object>() {
                    { "programbudgettypeid",  4},
                    { "name",  "REPACKAGE (BIAYA)"},
                    { "isapprovedbyprogramming",  false},
                    { "isreported",  false},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_program_budget_type",
                new RowData<ColumnName, object>() {
                    { "programbudgettypeid",  5},
                    { "name",  "FILLER"},
                    { "isapprovedbyprogramming",  false},
                    { "isreported",  false},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );

            return result;
        }
    }
}
