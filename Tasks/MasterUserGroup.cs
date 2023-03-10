using Microsoft.Data.SqlClient;
using Npgsql;
using SurplusMigrator.Libraries;
using SurplusMigrator.Models;
using SurplusMigrator.Tasks;
using System;
using System.Collections.Generic;
using System.Linq;

namespace SurplusMigrator.Tasks {
  class MasterUserGroup : _BaseTask {
        public MasterUserGroup(DbConnection_[] connections) : base(connections) {
            sources = new TableInfo[] {};
            destinations = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().name == "surplus").FirstOrDefault(),
                    //tablename = "master_user_group",
                    tablename = "usergroup",
                    columns = new string[] {
                        "usergroupid",
                        "name",
                        //"created_date",
                        //"created_by",
                        "is_disabled"
                    },
                    ids = new string[] { "usergroupid" }
                }
            };
        }

        protected override MappedData getStaticData() {
            MappedData result = new MappedData();

            result.addData(
                //"master_user_group",
                "usergroup",
                new RowData<ColumnName, object>() {
                    { "usergroupid",  1},
                    { "name",  "Administrator"},
                    //{ "created_date",  DateTime.Now},
                    //{ "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                //"master_user_group",
                "usergroup",
                new RowData<ColumnName, object>() {
                    { "usergroupid",  2},
                    { "name",  "User"},
                    //{ "created_date",  DateTime.Now},
                    //{ "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );

            return result;
        }
    }
}
