using Microsoft.Data.SqlClient;
using Npgsql;
using SurplusMigrator.Libraries;
using SurplusMigrator.Models;
using SurplusMigrator.Tasks;
using System;
using System.Collections.Generic;
using System.Linq;

namespace SurplusMigrator.Tasks {
  class MasterAdvertiserBrand : _BaseTask {
        public MasterAdvertiserBrand(DbConnection_[] connections) : base(connections) {
            sources = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().name == "e_frm").FirstOrDefault(),
                    tableName = "master_advertiserbrand",
                    columns = new string[] {
                        "advertiser_brand_id",
                        "advertiser_id",
                        "advertiser_brand_name",
                        "advertiser_brand_fullname",
                        "advertiser_brand_trfcode",
                        "advertiser_brand_active",
                        //"advertiser_name",
                    },
                    ids = new string[] { "advertiser_brand_id" }
                }
            };
            destinations = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().name == "surplus").FirstOrDefault(),
                    tableName = "master_advertiser_brand",
                    columns = new string[] {
                        "advertiserbrandid",
                        "name",
                        "fullname",
                        "trf_code",
                        "advertiserid",
                        "created_by",
                        "created_date",
                        "is_disabled",
                        "disabled_by",
                        "disabled_date",
                        "modified_by",
                        "modified_date",
                    },
                    ids = new string[] { "advertiserbrandid" }
                }
            };
        }

        protected override List<RowData<ColumnName, object>> getSourceData(Table[] sourceTables, int batchSize = defaultReadBatchSize) {
            return sourceTables.Where(a => a.tableName == "master_advertiserbrand").FirstOrDefault().getDatas(batchSize);
        }

        protected override MappedData mapData(List<RowData<ColumnName, object>> inputs) {
            MappedData result = new MappedData();

            foreach(RowData<ColumnName, object> data in inputs) {
                result.addData(
                    "master_advertiser_brand",
                    new RowData<ColumnName, object>() {
                        { "advertiserbrandid",  data["advertiser_brand_id"]},
                        { "name",  data["advertiser_brand_name"]},
                        { "fullname",  data["advertiser_brand_fullname"]},
                        { "trf_code",  data["advertiser_brand_trfcode"]},
                        { "advertiserid",  data["advertiser_id"]},
                        { "created_by",  DefaultValues.CREATED_BY },
                        { "created_date",  DateTime.Now},
                        { "is_disabled", !Utils.obj2bool(data["advertiser_brand_active"]) },
                        { "disabled_by",  null },
                        { "disabled_date",  null },
                        { "modified_by",  null },
                        { "modified_date",  null},
                    }
                );
            }

            return result;
        }
    }
}
