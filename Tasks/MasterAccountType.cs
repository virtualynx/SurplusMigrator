using Microsoft.Data.SqlClient;
using Npgsql;
using SurplusMigrator.Libraries;
using SurplusMigrator.Models;
using SurplusMigrator.Tasks;
using System;
using System.Collections.Generic;
using System.Linq;

namespace SurplusMigrator.Tasks {
  class MasterAccountType : _BaseTask {
        public MasterAccountType(DbConnection_[] connections) : base(connections) {
            sources = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().name == "e_frm").FirstOrDefault(),
                    tableName = "master_acctype",
                    columns = new string[] {
                        "acctype_id",
                        "acctype_name",
                        "acctypetype_id",
                        "acctype_isactive"
                    },
                    ids = new string[] { "acctypetype_id" }
                }
            };
            destinations = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().name == "surplus").FirstOrDefault(),
                    tableName = "master_account_type",
                    columns = new string[] {
                        "accounttypeid",
                        "name",
                        "accountsubtypeid",
                        "created_date",
                        "created_by",
                        "is_disabled"
                    },
                    ids = new string[] { "accounttypeid" }
                }
            };
        }

        protected override List<RowData<ColumnName, object>> getSourceData(Table[] sourceTables, int batchSize = defaultReadBatchSize) {
            return sourceTables.Where(a => a.tableName == "master_acctype").FirstOrDefault().getData(batchSize);
        }

        protected override MappedData mapData(List<RowData<ColumnName, object>> inputs) {
            MappedData result = new MappedData();

            foreach(RowData<ColumnName, object> data in inputs) {
                RowData<ColumnName, object> insertRow = new RowData<ColumnName, object>() {
                    { "accounttypeid",  data["acctype_id"]},
                    { "name",  data["acctype_name"]},
                    { "accountsubtypeid",  data["acctypetype_id"]},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", !Utils.obj2bool(data["acctype_isactive"]) }
                };
                result.addData("master_account_type", insertRow);
            }

            return result;
        }

        protected override void runDependencies() {
            new MasterAccountSubType(connections).run();
        }

        protected override MappedData getStaticData() {
            MappedData result = new MappedData();

            result.addData(
                "master_account_type",
                new RowData<ColumnName, object>() {
                    { "accounttypeid",  0},
                    { "name",  "Unknown"},
                    { "accountsubtypeid",  0},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_account_type",
                new RowData<ColumnName, object>() {
                    { "accounttypeid",  1},
                    { "name",  "Aktiva Lancar (Possibly)"},
                    { "accountsubtypeid",  10},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_account_type",
                new RowData<ColumnName, object>() {
                    { "accounttypeid",  2},
                    { "name",  "Harta Tetap (Possibly)"},
                    { "accountsubtypeid",  10},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );

            return result;
        }
    }
}
