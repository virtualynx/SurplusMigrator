using SurplusMigrator.Exceptions;
using System.Collections.Generic;

namespace SurplusMigrator.Models {
    class MappedData {
        private Dictionary<TableName, List<RowData<ColumnName, object>>> mappedData = new Dictionary<TableName, List<RowData<ColumnName, object>>>();
        private Dictionary<TableName, List<DbInsertFail>> errors = new Dictionary<string, List<DbInsertFail>> ();

        public void addData(string destinationTablename, RowData<ColumnName, object> data) {
            if(!mappedData.ContainsKey(destinationTablename)) {
                mappedData.Add(destinationTablename, new List<RowData<ColumnName, object>>());
            }

            mappedData[destinationTablename].Add(data);
        }

        public List<RowData<ColumnName, object>> getData(string destinationTablename) {
            if(!mappedData.ContainsKey(destinationTablename)) {
                throw new TaskConfigException("No mapped-data for tablename " + destinationTablename + " found");
            }
            return mappedData[destinationTablename];
        }

        public int Count() {
            return mappedData.Count;
        }

        public void addError(string destinationTablename, DbInsertFail error) {
            if(!errors.ContainsKey(destinationTablename)) {
                errors.Add(destinationTablename, new List<DbInsertFail>());
            }
            errors[destinationTablename].Add(error);
        }

        public List<DbInsertFail> getError(string destinationTablename) {
            if(!errors.ContainsKey(destinationTablename)) {
                return new List<DbInsertFail>();
            }
            return errors[destinationTablename];
        }
    }
}
