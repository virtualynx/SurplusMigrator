using Microsoft.Data.SqlClient;
using Npgsql;
using SurplusMigrator.Libraries;
using SurplusMigrator.Models;
using SurplusMigrator.Tasks;
using System;
using System.Collections.Generic;
using System.Linq;

namespace SurplusMigrator.Tasks {
  class MasterVendorType : _BaseTask {
        public MasterVendorType(DbConnection_[] connections) : base(connections) {
            sources = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().name == "e_frm").FirstOrDefault(),
                    tableName = "master_rekanantype",
                    columns = new string[] {
                        "rekanantype_id",
                        "rekanantype_Nama",
                        "rekanantype_Dekcripsi",
                        "rekanantype_active"
                    },
                    ids = new string[] { "rekanantype_id" }
                }
            };
            destinations = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().name == "surplus").FirstOrDefault(),
                    tableName = "master_vendor_type",
                    columns = new string[] {
                        "vendortypeid",
                        "name",
                        "descr",
                        "created_date",
                        "created_by",
                        "is_disabled"
                    },
                    ids = new string[] { "vendortypeid" }
                }
            };
        }

        protected override List<RowData<ColumnName, object>> getSourceData(Table[] sourceTables, int batchSize = defaultReadBatchSize) {
            return sourceTables.Where(a => a.tableName == "master_rekanantype").FirstOrDefault().getData(batchSize);
        }

        protected override MappedData mapData(List<RowData<ColumnName, object>> inputs) {
            MappedData result = new MappedData();

            foreach(RowData<ColumnName, object> data in inputs) {
                RowData<ColumnName, object> insertRow = new RowData<ColumnName, object>() {
                    { "vendortypeid",  data["rekanantype_id"]},
                    { "name",  data["rekanantype_Nama"]},
                    { "descr",  data["rekanantype_Dekcripsi"]},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", !Utils.obj2bool(data["rekanantype_active"]) }
                };
                result.addData("master_vendor_type", insertRow);
            }

            return result;
        }

        protected override MappedData getStaticData() {
            MappedData result = new MappedData();

            result.addData(
                "master_vendor_type",
                new RowData<ColumnName, object>() {
                    { "vendortypeid",  0},
                    { "name",  "Unknown"},
                    { "descr",  null},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_vendor_type",
                new RowData<ColumnName, object>() {
                    { "vendortypeid",  14},
                    { "name",  "Unknown 14"},
                    { "descr",  "Anomalies upon migrating master_vendor, found in master_vendor id= 11040, 11054, 11521, 11522, 11523, 11524, 11613, 11629"},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_vendor_type",
                new RowData<ColumnName, object>() {
                    { "vendortypeid",  711},
                    { "name",  "Unknown 711"},
                    { "descr",  "Anomalies upon migrating master_vendor, found in master_vendor id= 8355"},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );

            return result;
        }
    }
}
