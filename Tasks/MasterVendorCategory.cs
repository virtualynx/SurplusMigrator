using Microsoft.Data.SqlClient;
using Npgsql;
using SurplusMigrator.Libraries;
using SurplusMigrator.Models;
using SurplusMigrator.Tasks;
using System;
using System.Collections.Generic;
using System.Linq;

namespace SurplusMigrator.Tasks {
  class MasterVendorCategory : _BaseTask {
        public MasterVendorCategory(DbConnection_[] connections) : base(connections) {
            sources = new TableInfo[] {};
            destinations = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().dbname == "insosys").FirstOrDefault(),
                    tableName = "master_vendor_category",
                    columns = new string[] {
                        "vendorcategoryid",
                        "name",
                    },
                    ids = new string[] { "vendorcategoryid" }
                }
            };
        }

        public override List<RowData<ColumnName, object>> getSourceData(Table[] sourceTables, int batchSize = 5000) {
            return new List<RowData<string, object>>();
        }

        public override MappedData mapData(List<RowData<ColumnName, object>> inputs) {
            return new MappedData();
        }

        public override MappedData additionalStaticData() {
            MappedData result = new MappedData();
            
            result.addData(
                "master_vendor_category",
                new RowData<ColumnName, object>() {
                    { "vendorcategoryid",  1},
                    { "name",  "Individual"},
                }
            );
            result.addData(
                "master_vendor_category",
                new RowData<ColumnName, object>() {
                    { "vendorcategoryid",  2},
                    { "name",  "Company"},
                }
            );

            return result;
        }

        public override void runDependencies() {
            throw new NotImplementedException();
        }
    }
}
