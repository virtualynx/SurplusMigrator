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

        public override List<RowData<ColumnName, Data>> getSourceData(Table[] sourceTables, int batchSize = defaultBatchSize) {
            return new List<RowData<string, object>>();
        }

        public override MappedData mapData(List<RowData<ColumnName, Data>> inputs) {
            return new MappedData();
        }

        public override MappedData additionalStaticData() {
            MappedData result = new MappedData();

            result.addData(
                "master_journal_reference_type",
                new RowData<ColumnName, Data>() {
                    { "journalreferencetypeid", "ChangeUser"},
                    { "name",  "ChangeUser"},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_journal_reference_type",
                new RowData<ColumnName, Data>() {
                    { "journalreferencetypeid", "Jurnal AP"},
                    { "name",  "Jurnal AP"},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_journal_reference_type",
                new RowData<ColumnName, Data>() {
                    { "journalreferencetypeid", "jurnal BPB"},
                    { "name",  "jurnal BPB"},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_journal_reference_type",
                new RowData<ColumnName, Data>() {
                    { "journalreferencetypeid", "jurnal BPJ"},
                    { "name",  "jurnal BPJ"},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_journal_reference_type",
                new RowData<ColumnName, Data>() {
                    { "journalreferencetypeid", "Jurnal JV"},
                    { "name",  "Jurnal JV"},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_journal_reference_type",
                new RowData<ColumnName, Data>() {
                    { "journalreferencetypeid", "PAYMENT"},
                    { "name",  "PAYMENT"},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_journal_reference_type",
                new RowData<ColumnName, Data>() {
                    { "journalreferencetypeid", "RECEIPT"},
                    { "name",  "RECEIPT"},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_journal_reference_type",
                new RowData<ColumnName, Data>() {
                    { "journalreferencetypeid", "SALES"},
                    { "name",  "SALES"},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );
            result.addData(
                "master_journal_reference_type",
                new RowData<ColumnName, Data>() {
                    { "journalreferencetypeid", "SETTLEMENT"},
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
