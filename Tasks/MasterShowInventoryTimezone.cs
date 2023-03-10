using SurplusMigrator.Models;
using System;
using System.Collections.Generic;
using System.Linq;

namespace SurplusMigrator.Tasks {
    class MasterShowInventoryTimezone : _BaseTask {
        public MasterShowInventoryTimezone(DbConnection_[] connections) : base(connections) {
            sources = new TableInfo[] {};
            destinations = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().name == "surplus").FirstOrDefault(),
                    tablename = "master_show_inventory_timezone",
                    columns = new string[] {
                        "showinventorytimezoneid",
                        "name",
                        "starttime",
                        "endtime",
                        "created_date",
                        "created_by",
                        "is_disabled"
                    },
                    ids = new string[] { "showinventorytimezoneid" }
                }
            };
        }

        protected override MappedData getStaticData() {
            MappedData result = new MappedData();
            
            result.addData(
                "master_show_inventory_timezone",
                new RowData<ColumnName, object>() {
                    { "showinventorytimezoneid",  1},
                    { "name",  "Any Zones"},
                    { "starttime",  null},
                    { "endtime",  null},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_show_inventory_timezone",
                new RowData<ColumnName, object>() {
                    { "showinventorytimezoneid",  2},
                    { "name",  "Fringe 1"},
                    { "starttime",  null},
                    { "endtime",  null},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_show_inventory_timezone",
                new RowData<ColumnName, object>() {
                    { "showinventorytimezoneid",  3},
                    { "name",  "Fringe 2"},
                    { "starttime",  null},
                    { "endtime",  null},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_show_inventory_timezone",
                new RowData<ColumnName, object>() {
                    { "showinventorytimezoneid",  4},
                    { "name",  "Fringe 3"},
                    { "starttime",  null},
                    { "endtime",  null},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_show_inventory_timezone",
                new RowData<ColumnName, object>() {
                    { "showinventorytimezoneid",  5},
                    { "name",  "Prime 1"},
                    { "starttime",  null},
                    { "endtime",  null},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_show_inventory_timezone",
                new RowData<ColumnName, object>() {
                    { "showinventorytimezoneid",  6},
                    { "name",  "Prime 2"},
                    { "starttime",  null},
                    { "endtime",  null},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_show_inventory_timezone",
                new RowData<ColumnName, object>() {
                    { "showinventorytimezoneid",  7},
                    { "name",  "Prime 3"},
                    { "starttime",  null},
                    { "endtime",  null},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_show_inventory_timezone",
                new RowData<ColumnName, object>() {
                    { "showinventorytimezoneid",  8},
                    { "name",  "Prime 4"},
                    { "starttime",  null},
                    { "endtime",  null},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_show_inventory_timezone",
                new RowData<ColumnName, object>() {
                    { "showinventorytimezoneid",  9},
                    { "name",  "Shoulder 1"},
                    { "starttime",  null},
                    { "endtime",  null},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_show_inventory_timezone",
                new RowData<ColumnName, object>() {
                    { "showinventorytimezoneid",  10},
                    { "name",  "Shoulder 2"},
                    { "starttime",  null},
                    { "endtime",  null},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );

            return result;
        }
    }
}
