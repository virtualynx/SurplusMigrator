using Microsoft.Data.SqlClient;
using Npgsql;
using SurplusMigrator.Interfaces;
using SurplusMigrator.Tasks;
using System;
using System.Collections.Generic;
using System.Linq;

namespace SurplusMigrator.Models
{
  class MasterVendor : _BaseTask {
        public MasterVendor(DbConnection_[] connections) {
            sources = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().dbname == "E_FRM").FirstOrDefault(),
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
                    connection = connections.Where(a => a.GetDbLoginInfo().dbname == "insosys").FirstOrDefault(),
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

        public override List<RowData<ColumnName, Data>> getSourceData(Table[] sourceTables) {
            return sourceTables.Where(a => a.tableName == "master_rekanan").FirstOrDefault().getDatas();
        }

        public override MappedData mapData(List<RowData<ColumnName, Data>> inputs) {
            MappedData result = new MappedData();

            foreach(RowData<ColumnName, Data> data in inputs) {
                RowData<ColumnName, Data> insertRow = new RowData<ColumnName, Data>() {
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
                    { "created_by",  new AuthInfo(){ FullName = data["rekanan_Create_by"]!=null? data["rekanan_Create_by"].ToString(): null } },
                    { "is_disabled", !Utils.obj2bool(data["rekanan_active"]) },
                    { "modified_date",  data["rekanan_modify_date"]},
                    { "modified_by",  new AuthInfo(){ FullName = data["rekanan_modify_by"]!=null? data["rekanan_modify_by"].ToString(): null }},
                };
                result.addData("master_vendor", insertRow);
            }

            return result;
        }

        public override MappedData additionalStaticData() {
            return null;
        }
    }
}
