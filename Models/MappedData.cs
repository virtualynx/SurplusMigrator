using SurplusMigrator.Exceptions;
using System;
using System.Collections.Generic;

namespace SurplusMigrator.Models
{
  class MappedData {
        private Dictionary<TableName, List<RowData<ColumnName, Data>>> mappedData = new Dictionary<TableName, List<RowData<ColumnName, Data>>>();

        public void addData(string destinationTablename, RowData<ColumnName, Data> data) {
            if(!mappedData.ContainsKey(destinationTablename)) {
                mappedData.Add(destinationTablename, new List<RowData<ColumnName, Data>>());
            }

            mappedData[destinationTablename].Add(data);
        }

        public List<RowData<ColumnName, Data>> getData(string tablename) {
            if(!mappedData.ContainsKey(tablename)) {
                throw new TaskConfigException("No mapped-data for tablename " + tablename + " found");
            }
            return mappedData[tablename];
        }

        public int Count() {
            return mappedData.Count;
        }
    }
}
