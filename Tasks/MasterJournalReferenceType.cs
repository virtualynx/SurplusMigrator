using SurplusMigrator.Models;
using System;
using System.Collections.Generic;
using System.Linq;

namespace SurplusMigrator.Tasks {
    class MasterJournalReferenceType : _BaseTask {
        public MasterJournalReferenceType(DbConnection_[] connections) : base(connections) {
            sources = new TableInfo[] {};
            destinations = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().dbname == "insosys").FirstOrDefault(),
                    tableName = "master_journal_reference_type",
                    columns = new string[] {
                        "journalreferencetypeid",
                        "name",
                        "created_date",
                        "created_by",
                        "is_disabled"
                    },
                    ids = new string[] { "journalreferencetypeid" }
                }
            };
        }

        public override List<RowData<ColumnName, object>> getSourceData(Table[] sourceTables, int batchSize = defaultReadBatchSize) {
            return new List<RowData<string, object>>();
        }

        public override MappedData mapData(List<RowData<ColumnName, object>> inputs) {
            return new MappedData();
        }

        public override MappedData additionalStaticData() {
            MappedData result = new MappedData();

            result.addData(
                "master_journal_reference_type",
                new RowData<ColumnName, object>() {
                    { "journalreferencetypeid", "change_user"},
                    { "name",  "Change User"},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_journal_reference_type",
                new RowData<ColumnName, object>() {
                    { "journalreferencetypeid", "jurnal_ap"},
                    { "name",  "Jurnal AP"},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_journal_reference_type",
                new RowData<ColumnName, object>() {
                    { "journalreferencetypeid", "jurnal_bpb"},
                    { "name",  "jurnal BPB"},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_journal_reference_type",
                new RowData<ColumnName, object>() {
                    { "journalreferencetypeid", "jurnal_bpj"},
                    { "name",  "jurnal BPJ"},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_journal_reference_type",
                new RowData<ColumnName, object>() {
                    { "journalreferencetypeid", "jurnal_jv"},
                    { "name",  "Jurnal JV"},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_journal_reference_type",
                new RowData<ColumnName, object>() {
                    { "journalreferencetypeid", "payment"},
                    { "name",  "PAYMENT"},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_journal_reference_type",
                new RowData<ColumnName, object>() {
                    { "journalreferencetypeid", "receipt"},
                    { "name",  "RECEIPT"},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_journal_reference_type",
                new RowData<ColumnName, object>() {
                    { "journalreferencetypeid", "sales"},
                    { "name",  "SALES"},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_journal_reference_type",
                new RowData<ColumnName, object>() {
                    { "journalreferencetypeid", "settlement"},
                    { "name",  "SETTLEMENT"},
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
