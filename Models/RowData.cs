using System;
using System.Collections.Generic;

namespace SurplusMigrator.Models
{
    /// <summary>
    /// Represents sets of column-data mapping.
    /// </summary>
    class RowData<ColumnName, Data> : Dictionary<string, object> {
        public RowData() : base() { }
        public RowData(int capacity) : base(capacity) { }
    }
}
