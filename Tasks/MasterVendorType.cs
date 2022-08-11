using Microsoft.Data.SqlClient;
using Npgsql;
using SurplusMigrator.Interfaces;
using SurplusMigrator.Tasks;
using System;
using System.Collections.Generic;
using System.Linq;

namespace SurplusMigrator.Models
{
  class MasterVendorType : _BaseTask {
        public MasterVendorType(DbConnection_[] connections) {
            sources = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().dbname == "E_FRM").FirstOrDefault(),
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
                    connection = connections.Where(a => a.GetDbLoginInfo().dbname == "insosys").FirstOrDefault(),
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

        public override List<RowData<ColumnName, Data>> getSourceData(Table[] sourceTables) {
            return sourceTables.Where(a => a.tableName == "master_rekanantype").FirstOrDefault().getDatas();
        }

        public override MappedData mapData(List<RowData<ColumnName, Data>> inputs) {
            MappedData result = new MappedData();

            foreach(RowData<ColumnName, Data> data in inputs) {
                RowData<ColumnName, Data> insertRow = new RowData<ColumnName, Data>() {
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

        public override MappedData additionalStaticData() {
            MappedData result = new MappedData();

            result.addData(
                "master_vendor_type",
                new RowData<ColumnName, Data>() {
                    { "vendortypeid",  0},
                    { "name",  "Unknown"},
                    { "descr",  null},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );

            return result;
        }
    }
}
