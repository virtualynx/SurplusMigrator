using Serilog;
using SurplusMigrator.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SurplusMigrator.Tasks {
    abstract class _BaseTask {
        public TableInfo source = null;
        public TableInfo destination = null;

        public bool run() {
            bool allSuccess = true;

            Table sourceTable = new Table() {
                connection = source.connection,
                tableName = source.tableName,
                columns = source.columns,
                ids = source.ids,
                batchSize = source.batchSize
            };

            Table destinationTable = new Table() {
                connection = destination.connection,
                tableName = destination.tableName,
                columns = destination.columns,
                ids = destination.ids,
                batchSize = destination.batchSize
            };

            List<RowData<string, object>> fetchedData;
            while((fetchedData = sourceTable.getDatas()).Count > 0) {
                List<DbInsertError> errors = destinationTable.insertData(mapData(fetchedData));
                if(errors.Count > 0) {
                    allSuccess = false;
                    break;
                }
            }

            return allSuccess;
        }

        public abstract List<RowData<string, object>> mapData(List<RowData<string, object>> inputs);
    }
}
