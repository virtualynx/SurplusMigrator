using SurplusMigrator.Models;
using System;
using System.Collections.Generic;
using System.Linq;

namespace SurplusMigrator.Tasks {
    class MasterSource : _BaseTask {
        public MasterSource(DbConnection_[] connections) : base(connections) {
            sources = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().dbname == "E_FRM").FirstOrDefault(),
                    tableName = "master_source",
                    columns = new string[] {
                        "source_id",
                        "source_descr",
                        "type_id",
                    },
                    ids = new string[] { "source_id" }
                }
            };
            destinations = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().dbname == "insosys").FirstOrDefault(),
                    tableName = "master_source",
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

        public override List<RowData<ColumnName, Data>> getSourceData(Table[] sourceTables, int batchSize = 5000) {
            return sourceTables.Where(a => a.tableName == "master_source").FirstOrDefault().getDatas(batchSize);
        }

        public override MappedData mapData(List<RowData<ColumnName, Data>> inputs) {
            MappedData result = new MappedData();

            foreach(RowData<ColumnName, Data> data in inputs) {
                RowData<ColumnName, Data> insertRow = new RowData<ColumnName, Data>() {
                    { "sourceid",  data["source_id"]},
                    { "description",  data["source_descr"]},
                    { "transactiontypeid",  data["type_id"]},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                };
                result.addData("master_source", insertRow);
            }

            return result;
        }

        public override MappedData additionalStaticData() {
            MappedData result = new MappedData();

            result.addData(
                "master_source",
                new RowData<ColumnName, Data>() {
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
                new RowData<ColumnName, Data>() {
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
                new RowData<ColumnName, Data>() {
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
                new RowData<ColumnName, Data>() {
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
                new RowData<ColumnName, Data>() {
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
                new RowData<ColumnName, Data>() {
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
                new RowData<ColumnName, Data>() {
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
                new RowData<ColumnName, Data>() {
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
                new RowData<ColumnName, Data>() {
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
                new RowData<ColumnName, Data>() {
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
                new RowData<ColumnName, Data>() {
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
                new RowData<ColumnName, Data>() {
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
                new RowData<ColumnName, Data>() {
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
                new RowData<ColumnName, Data>() {
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
                new RowData<ColumnName, Data>() {
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
                new RowData<ColumnName, Data>() {
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
                new RowData<ColumnName, Data>() {
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
                new RowData<ColumnName, Data>() {
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
                new RowData<ColumnName, Data>() {
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
                new RowData<ColumnName, Data>() {
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
                new RowData<ColumnName, Data>() {
                    { "sourceid",  "RV-ListBPB"},
                    { "description",  "RV-ListBPB"},
                    { "transactiontypeid",  "RV"},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            //result.addData(
            //    "master_source",
            //    new RowData<ColumnName, Data>() {
            //        { "sourceid",  "RV-ListBPJ"},
            //        { "description",  "RV-ListBPJ"},
            //        { "transactiontypeid",  "RV"},
            //        { "created_date",  DateTime.Now},
            //        { "created_by",  DefaultValues.CREATED_BY},
            //        { "is_disabled", false }
            //    }
            //);
            result.addData(
                "master_source",
                new RowData<ColumnName, Data>() {
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

        public override void runDependencies() {
        }
    }
}
