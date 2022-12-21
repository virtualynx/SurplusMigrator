using Microsoft.Data.SqlClient;
using Npgsql;
using SurplusMigrator.Libraries;
using SurplusMigrator.Models;
using SurplusMigrator.Tasks;
using System;
using System.Collections.Generic;
using System.Linq;

namespace SurplusMigrator.Tasks {
  class MasterModuleGroup : _BaseTask {
        public MasterModuleGroup(DbConnection_[] connections) : base(connections) {
            sources = new TableInfo[] {};
            destinations = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().dbname == "insosys").FirstOrDefault(),
                    tableName = "master_module_group",
                    columns = new string[] {
                        "modulegroupid",
                        "name",
                        "created_date",
                        "created_by",
                        "is_disabled"
                    },
                    ids = new string[] { "modulegroupid" }
                }
            };
        }

        protected override MappedData getStaticData() {
            MappedData result = new MappedData();

            result.addData(
                "master_module_group",
                new RowData<ColumnName, object>() {
                    { "modulegroupid",  1},
                    { "name",  "Administrator"},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_module_group",
                new RowData<ColumnName, object>() {
                    { "modulegroupid",  2},
                    { "name",  "User"},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );

            return result;
        }
    }
}
