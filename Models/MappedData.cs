using SurplusMigrator.Exceptions;
using System.Collections.Generic;

namespace SurplusMigrator.Models {
    class MappedData {
        private Dictionary<TableName, List<RowData<ColumnName, object>>> _mappedData = new Dictionary<TableName, List<RowData<ColumnName, object>>>();
        private Dictionary<TableName, List<DbInsertFail>> _errors = new Dictionary<string, List<DbInsertFail>> ();

        public void addData(string destinationTablename, RowData<ColumnName, object> data) {
            if(!_mappedData.ContainsKey(destinationTablename)) {
                _mappedData.Add(destinationTablename, new List<RowData<ColumnName, object>>());
            }

            _mappedData[destinationTablename].Add(data);
        }

        public List<RowData<ColumnName, object>> getData(string destinationTablename) {
            if(!_mappedData.ContainsKey(destinationTablename)) {
                throw new TaskConfigException("No mapped-data for tablename " + destinationTablename + " found");
            }
            return _mappedData[destinationTablename];
        }

        public int Count() {
            return _mappedData.Count;
        }

        public void addError(string destinationTablename, DbInsertFail error) {
            if(!_errors.ContainsKey(destinationTablename)) {
                _errors.Add(destinationTablename, new List<DbInsertFail>());
            }
            _errors[destinationTablename].Add(error);
        }

        public void addErrors(string destinationTablename, DbInsertFail[] errors) {
            if(!_errors.ContainsKey(destinationTablename)) {
                _errors.Add(destinationTablename, new List<DbInsertFail>());
            }
            _errors[destinationTablename].AddRange(errors);
        }

        public List<DbInsertFail> getError(string destinationTablename) {
            if(!_errors.ContainsKey(destinationTablename)) {
                return new List<DbInsertFail>();
            }
            return _errors[destinationTablename];
        }

        public List<DbInsertFail> getErrors() {
            List<DbInsertFail> result = new List<DbInsertFail>();

            foreach(KeyValuePair<TableName, List<DbInsertFail>> entry in _errors) {
                result.AddRange(entry.Value);
            }

            return result;
        }
    }
}
