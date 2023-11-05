using Microsoft.EntityFrameworkCore.Metadata.Internal;
using Microsoft.Office.Interop.Excel;
using SurplusMigrator.Exceptions.Gen21;
using SurplusMigrator.Libraries;
using SurplusMigrator.Models;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;

namespace SurplusMigrator.Tasks {
    class _ModifyColumnValues : _BaseTask {
        private DbConnection_ targetConnection;
        private Gen21Integration gen21;

        private const int DEFAULT_BATCH_SIZE = 500;

        private string table = null;
        private string column_name = null;
        private string column_value = null;

        public _ModifyColumnValues(DbConnection_[] connections) : base(connections) {
            sources = new TableInfo[] {
            };
            destinations = new TableInfo[] {
            };

            string[] mandatoryOptions = new string[] { 
                "table",
                "column_name",
                "filename"
            };

            foreach(string opt in mandatoryOptions) {
                if(getOptions(opt) == null) {
                    throw new Exception("option \""+opt+"\" is mandatory to have");
                }
            }

            table = getOptions("table");
            column_name = getOptions("column_name");
            column_value = getOptions("column_value");

            Console.WriteLine("\n");
            MyConsole.Information(
                "Column <column_name> in table <table> value will be modified"
                .Replace("<column_name>", column_name)
                .Replace("<table>", table)
            );
            Console.WriteLine();
            Console.Write("Continue (Y/N)? ");
            string choice = Console.ReadLine();
            if(choice.ToLower() != "y") {
                throw new Exceptions.JobAbortedException();
            }

            targetConnection = connections.Where(a => a.GetDbLoginInfo().name == "surplus").First();
        }

        protected override void onFinished() {
            string[] primaryKeys;

            if(getOptions("column_id") != null) {
                primaryKeys = getOptions("column_id").Split(",").Select(a => a.Trim()).ToArray();
            } else {
                primaryKeys = QueryUtils.getPrimaryKeys(targetConnection, table);
            }
            if(primaryKeys.Length == 0 || primaryKeys.Length > 1) {
                throw new Exception("Invalid primary-key number: " + primaryKeys.Length);
            }

            var datas = getDataFromExcel();

            DbTransaction transaction = targetConnection.GetDbConnection().BeginTransaction();

            try {
                foreach(var loopData in datas) {
                    string queryUpdate = @"
                        update ""<tablename>"" 
                        set 
                            ""<column_name>"" = @new_value
                        where ""<filter_column>"" = @column_id;
                    "
                        .Replace("<tablename>", table)
                        .Replace("<column_name>", column_name)
                        .Replace("<filter_column>", primaryKeys[0])
                    ;

                    var parameters = new Dictionary<string, object> {
                        { "@new_value", loopData["new_value"] },
                        { "@column_id", loopData["column_id"] },
                    };
                    QueryUtils.executeQuery(targetConnection, queryUpdate, parameters, transaction);
                }

                transaction.Commit();
            } catch(Exception ex) {
                transaction.Rollback();
                throw;
            }
        }

        public RowData<string, object>[] getDataFromExcel() {
            ExcelColumn[] columns = new ExcelColumn[] {
                new ExcelColumn(){ name="column_id", ordinal=0 },
                new ExcelColumn(){ name="new_value", ordinal=1 },
            };

            List<RowData<string, object>> data = Utils.getDataFromExcel(getOptions("filename"), columns);

            return data.ToArray();
        }
    }
}
