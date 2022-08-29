using Microsoft.Data.SqlClient;
using Npgsql;
using SurplusMigrator.Libraries;
using SurplusMigrator.Models;
using SurplusMigrator.Tasks;
using System;
using System.Collections.Generic;
using System.Linq;

namespace SurplusMigrator.Tasks {
  class MasterTvProgramType : _BaseTask {
        public MasterTvProgramType(DbConnection_[] connections) : base(connections) {
            sources = new TableInfo[] {};
            destinations = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().dbname == "insosys").FirstOrDefault(),
                    tableName = "master_tv_program_type",
                    columns = new string[] {
                        "tvprogramtypeid",
                        "name",
                        "created_date",
                        "created_by",
                        "is_disabled"
                    },
                    ids = new string[] { "tvprogramtypeid" }
                }
            };
        }

        protected override MappedData getStaticData() {
            MappedData result = new MappedData();
            
            result.addData(
                "master_tv_program_type",
                new RowData<ColumnName, object>() {
                    { "tvprogramtypeid",  "PG"},
                    { "name",  "Program"},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_tv_program_type",
                new RowData<ColumnName, object>() {
                    { "tvprogramtypeid",  "NP"},
                    { "name",  "Non Program"},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );

            return result;
        }
    }
}
