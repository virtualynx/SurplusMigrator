using SurplusMigrator.Models;
using System;
using System.Collections.Generic;
using System.Linq;

namespace SurplusMigrator.Tasks {
    class _MasterTrafficAdvertiser : _BaseTask {
        public _MasterTrafficAdvertiser(DbConnection_[] connections) : base(connections) {
            sources = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().name == "e_frm").FirstOrDefault(),
                    tableName = "master_trafficadvertiser",
                    columns = new string[] {
                        "code",
                        "trafficadvertiser_line",
                        "trafficadvertiser_name",
                        "trafficadvertiser_isactive"
                    },
                    ids = new string[] { "code", "trafficadvertiser_line" }
                }
            };
            destinations = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().name == "surplus").FirstOrDefault(),
                    tableName = "master_traffic_advertiser",
                    columns = new string[] {
                        "sourceid",
                        "description",
                        "transactiontypeid",
                        "created_date",
                        "created_by",
                        "is_disabled"
                    },
                    ids = new string[] { "sourceid" }
                }
            };
        }

        protected override List<RowData<ColumnName, object>> getSourceData(Table[] sourceTables, int batchSize = defaultReadBatchSize) {
            return sourceTables.Where(a => a.tableName == "master_source").FirstOrDefault().getData(batchSize);
        }

        public override MappedData mapData(List<RowData<ColumnName, object>> inputs) {
            MappedData result = new MappedData();

            foreach(RowData<ColumnName, object> data in inputs) {
                result.addData(
                    "master_source",
                    new RowData<ColumnName, object>() {
                        { "sourceid",  data["source_id"]},
                        { "description",  data["source_descr"]},
                        { "transactiontypeid",  data["type_id"]},
                        { "created_date",  DateTime.Now},
                        { "created_by",  DefaultValues.CREATED_BY},
                        { "is_disabled", false }
                    }
                );
            }

            return result;
        }

        protected override MappedData getStaticData() {
            MappedData result = new MappedData();

            result.addData(
                "master_source",
                new RowData<ColumnName, object>() {
                    { "sourceid",  "AP-ListManual"},
                    { "description",  "AP-ListManual"},
                    { "transactiontypeid",  "AP"},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_source",
                new RowData<ColumnName, object>() {
                    { "sourceid",  "AP-ListPayment"},
                    { "description",  "AP-ListPayment"},
                    { "transactiontypeid",  "AP"},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_source",
                new RowData<ColumnName, object>() {
                    { "sourceid",  "AP-ListPV"},
                    { "description",  "AP-ListPV"},
                    { "transactiontypeid",  "AP"},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_source",
                new RowData<ColumnName, object>() {
                    { "sourceid",  "AP-ListSA"},
                    { "description",  "AP-ListSA"},
                    { "transactiontypeid",  "AP"},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_source",
                new RowData<ColumnName, object>() {
                    { "sourceid",  "AP-Migrasi"},
                    { "description",  "AP-Migrasi"},
                    { "transactiontypeid",  "AP"},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_source",
                new RowData<ColumnName, object>() {
                    { "sourceid",  "CN-ListSA"},
                    { "description",  "CN-ListSA"},
                    { "transactiontypeid",  "CN"},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_source",
                new RowData<ColumnName, object>() {
                    { "sourceid",  "JV-Asset"},
                    { "description",  "JV-Asset"},
                    { "transactiontypeid",  "JV"},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_source",
                new RowData<ColumnName, object>() {
                    { "sourceid",  "JV-ListAP"},
                    { "description",  "JV-ListAP"},
                    { "transactiontypeid",  "JV"},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_source",
                new RowData<ColumnName, object>() {
                    { "sourceid",  "JV-ListPV"},
                    { "description",  "JV-ListPV"},
                    { "transactiontypeid",  "JV"},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_source",
                new RowData<ColumnName, object>() {
                    { "sourceid",  "JV-Migrasi"},
                    { "description",  "JV-Migrasi"},
                    { "transactiontypeid",  "JV"},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_source",
                new RowData<ColumnName, object>() {
                    { "sourceid",  "JV-NettOff"},
                    { "description",  "JV-NettOff"},
                    { "transactiontypeid",  "JV"},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_source",
                new RowData<ColumnName, object>() {
                    { "sourceid",  "JV-Payment"},
                    { "description",  "JV-Payment"},
                    { "transactiontypeid",  "JV"},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_source",
                new RowData<ColumnName, object>() {
                    { "sourceid",  "JV-Reval"},
                    { "description",  "JV-Reval"},
                    { "transactiontypeid",  "JV"},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_source",
                new RowData<ColumnName, object>() {
                    { "sourceid",  "OC-Migrasi"},
                    { "description",  "OC-Migrasi"},
                    { "transactiontypeid",  "OC"},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_source",
                new RowData<ColumnName, object>() {
                    { "sourceid",  "OR-Billing"},
                    { "description",  "OR-Billing"},
                    { "transactiontypeid",  "OR"},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_source",
                new RowData<ColumnName, object>() {
                    { "sourceid",  "OR-FullRefund"},
                    { "description",  "OR-FullRefund"},
                    { "transactiontypeid",  "OR"},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_source",
                new RowData<ColumnName, object>() {
                    { "sourceid",  "OR-Migrasi"},
                    { "description",  "OR-Migrasi"},
                    { "transactiontypeid",  "OR"},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_source",
                new RowData<ColumnName, object>() {
                    { "sourceid",  "PV-ListOR"},
                    { "description",  "PV-ListOR"},
                    { "transactiontypeid",  "PV"},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_source",
                new RowData<ColumnName, object>() {
                    { "sourceid",  "PV-ListST"},
                    { "description",  "PV-ListST"},
                    { "transactiontypeid",  "PV"},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_source",
                new RowData<ColumnName, object>() {
                    { "sourceid",  "PV-Migrasi"},
                    { "description",  "PV-Migrasi"},
                    { "transactiontypeid",  "PV"},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_source",
                new RowData<ColumnName, object>() {
                    { "sourceid",  "RV-ListBPB"},
                    { "description",  "RV-ListBPB"},
                    { "transactiontypeid",  "RV"},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_source",
                new RowData<ColumnName, object>() {
                    { "sourceid",  "RV-ListBPJ"},
                    { "description",  "RV-ListBPJ"},
                    { "transactiontypeid",  "RV"},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_source",
                new RowData<ColumnName, object>() {
                    { "sourceid",  "ST-ListPV-Manual"},
                    { "description",  "ST-ListPV-Manual"},
                    { "transactiontypeid",  "ST"},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );

            return result;
        }
    }
}
