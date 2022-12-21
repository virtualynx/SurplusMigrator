using Microsoft.Data.SqlClient;
using Npgsql;
using SurplusMigrator.Libraries;
using SurplusMigrator.Models;
using SurplusMigrator.Tasks;
using System;
using System.Collections.Generic;
using System.Linq;

namespace SurplusMigrator.Tasks {
  class RelationModule_ModuleGroup : _BaseTask {
        public RelationModule_ModuleGroup(DbConnection_[] connections) : base(connections) {
            DbConnection_ surplusDev = new DbConnection_(new DbLoginInfo() { 
                host = "172.16.123.121",
                port = 5432,
                username = "postgres",
                password = "initrans7",
                dbname = "insosys",
                schema = "dev",
                type = "POSTGRESQL"
            });

            sources = new TableInfo[] {
                new TableInfo() {
                    connection = surplusDev,
                    tableName = "relation_module_modulegroup",
                    columns = new string[] {
                        "moduleid",
                        "modulegroupid",
                        "create",
                        "read",
                        "update",
                        "delete",
                        "activate",
                        "approve",
                        "type"
                    },
                    ids = new string[] { "moduleid", "modulegroupid" }
                }
            };
            destinations = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().dbname == "insosys").FirstOrDefault(),
                    tableName = "relation_module_modulegroup",
                    columns = new string[] {
                        "moduleid",
                        "modulegroupid",
                        "create",
                        "read",
                        "update",
                        "delete",
                        "activate",
                        "approve",
                        "type"
                    },
                    ids = new string[] { "moduleid", "modulegroupid" }
                }
            };
        }

        protected override List<RowData<ColumnName, object>> getSourceData(Table[] sourceTables, int batchSize = defaultReadBatchSize) {
            return sourceTables.Where(a => a.tableName == "relation_module_modulegroup").FirstOrDefault().getDatas(batchSize);
        }

        protected override MappedData mapData(List<RowData<ColumnName, object>> inputs) {
            MappedData result = new MappedData();

            foreach(RowData<ColumnName, object> data in inputs) {
                RowData<ColumnName, object> insertRow = new RowData<ColumnName, object>() {
                    { "moduleid",  data["moduleid"]},
                    { "modulegroupid",  data["modulegroupid"]},
                    { "create",  data["create"]},
                    { "read",  data["read"]},
                    { "update",  data["update"]},
                    { "delete",  data["delete"]},
                    { "activate",  data["activate"]},
                    { "approve",  data["approve"]},
                    { "type",  data["type"]},
                };
                result.addData("relation_module_modulegroup", insertRow);
            }

            return result;
        }
    }
}
