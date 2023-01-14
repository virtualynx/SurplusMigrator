using Microsoft.Data.SqlClient;
using Npgsql;
using SurplusMigrator.Libraries;
using SurplusMigrator.Models;
using SurplusMigrator.Tasks;
using System;
using System.Collections.Generic;
using System.Linq;

namespace SurplusMigrator.Tasks {
  class MasterVendor : _BaseTask {
        public MasterVendor(DbConnection_[] connections) : base(connections) {
            sources = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().name == "e_frm").FirstOrDefault(),
                    tableName = "master_rekanan",
                    columns = new string[] {
                        "rekanan_id",
                        "rekanan_name",
                        "rekanan_badanhukum",
                        "rekanan_namereport",
                        "rekanantype_id",
                        "rekanan_Addr1",
                        "rekanan_Addr2",
                        "rekanan_city",
                        "rekanan_telp",
                        "rekanan_fax",
                        "rekanan_email",
                        "rekanan_NPWP",
                        "rekanan_Create_by",
                        "rekanan_Create_dt",
                        "rekanan_active",
                        "rekanan_Bill",
                        "rekanan_taxprefix",
                        "rekanan_pkpname",
                        "rekanan_salesperson",
                        "rekanan_trf",
                        "rekanan_invsign",
                        "rekanan_invsignpos",
                        "rekanan_taxsign",
                        "rekanan_taxsignpos",
                        "rekanan_modify_by",
                        "rekanan_modify_date",
                    },
                    ids = new string[] { "rekanan_id" }
                }
            };
            destinations = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().name == "surplus").FirstOrDefault(),
                    tableName = "master_vendor",
                    columns = new string[] {
                        "vendorid",
                        "name",
                        "legalentity",
                        "namereport",
                        "address1",
                        "address2",
                        "city",
                        "telp",
                        "fax",
                        "email",
                        "npwp",
                        "bill",
                        "taxprefix",
                        "pkpname",
                        "salesperson",
                        "trf",
                        "invsign",
                        "invsignpos",
                        "taxsign",
                        "taxsignpos",
                        "vendortypeid",
                        "vendorcategoryid",
                        "created_date",
                        "created_by",
                        "is_disabled",
                        "modified_date",
                        "modified_by",
                    },
                    ids = new string[] { "vendorid" }
                }
            };
        }

        protected override List<RowData<ColumnName, object>> getSourceData(Table[] sourceTables, int batchSize = defaultReadBatchSize) {
            return sourceTables.Where(a => a.tableName == "master_rekanan").FirstOrDefault().getDatas(batchSize);
        }

        protected override MappedData mapData(List<RowData<ColumnName, object>> inputs) {
            MappedData result = new MappedData();

            foreach(RowData<ColumnName, object> data in inputs) {
                RowData<ColumnName, object> insertRow = new RowData<ColumnName, object>() {
                    { "vendorid",  data["rekanan_id"]},
                    { "name",  data["rekanan_name"]},
                    { "legalentity",  data["rekanan_badanhukum"]},
                    { "namereport",  data["rekanan_namereport"]},
                    { "address1",  data["rekanan_Addr1"]},
                    { "address2",  data["rekanan_Addr2"]},
                    { "city",  data["rekanan_city"]},
                    { "telp",  data["rekanan_telp"]},
                    { "fax",  data["rekanan_fax"]},
                    { "email",  data["rekanan_email"]},
                    { "npwp",  data["rekanan_NPWP"]},
                    { "bill",  data["rekanan_Bill"]!=null? data["rekanan_Bill"]: 0},
                    { "taxprefix",  data["rekanan_taxprefix"]},
                    { "pkpname",  data["rekanan_pkpname"]},
                    { "salesperson",  data["rekanan_salesperson"]!=null? data["rekanan_salesperson"]: 0},
                    { "trf",  data["rekanan_trf"]},
                    { "invsign",  data["rekanan_invsign"]},
                    { "invsignpos",  data["rekanan_invsignpos"]},
                    { "taxsign",  data["rekanan_taxsign"]},
                    { "taxsignpos",  data["rekanan_taxsignpos"]},
                    { "vendortypeid",  data["rekanantype_id"]},
                    { "vendorcategoryid",  1},
                    { "created_date",  data["rekanan_Create_dt"]},
                    { "created_by", getAuthInfo(data["rekanan_Create_by"], true) },
                    { "is_disabled", !Utils.obj2bool(data["rekanan_active"]) },
                    { "modified_date",  data["rekanan_modify_date"]},
                    { "modified_by", getAuthInfo(data["rekanan_modify_by"]) },
                };
                result.addData("master_vendor", insertRow);
            }

            return result;
        }

        protected override MappedData getStaticData() {
            MappedData result = new MappedData();

            //found in TransactionSalesOrder's reference
            result.addData(
                "master_vendor",
                new RowData<ColumnName, object>() {
                    { "vendorid",  0},
                    { "name",  "Unknown"},
                    { "legalentity",  null},
                    { "namereport",  null},
                    { "address1",  null},
                    { "address2",  null},
                    { "city",  null},
                    { "telp",  null},
                    { "fax",  null},
                    { "email",  null},
                    { "npwp",  null},
                    { "bill",  0},
                    { "taxprefix",  null},
                    { "pkpname",  null},
                    { "salesperson",  0},
                    { "trf",  null},
                    { "invsign",  null},
                    { "invsignpos",  null},
                    { "taxsign",  null},
                    { "taxsignpos",  null},
                    { "vendortypeid",  0},
                    { "vendorcategoryid",  null},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY },
                    { "is_disabled", false },
                    { "modified_date",  null},
                    { "modified_by",  null},
                }
            );

            return result;
        }
    }
}
