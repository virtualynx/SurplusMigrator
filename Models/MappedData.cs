using System;
using System.Collections.Generic;

namespace SurplusMigrator.Models
{
  class MappedData {
        private Dictionary<string, List<RowData<ColumnName, Data>>> mappedData = new Dictionary<string, List<RowData<ColumnName, Data>>>();

        public void addData(string destinationTablename, RowData<ColumnName, Data> data) {
            if(!mappedData.ContainsKey(destinationTablename)) {
                mappedData.Add(destinationTablename, new List<RowData<ColumnName, Data>>());
            }

            mappedData[destinationTablename].Add(data);
        }

        public List<RowData<ColumnName, Data>> getData(string tablename) {
            return mappedData[tablename];
        }
    }
}
