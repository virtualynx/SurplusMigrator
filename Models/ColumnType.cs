using System;
using System.Collections.Generic;

namespace SurplusMigrator.Models
{
    /// <summary>
    /// Represents sets of column-datatype mapping.
    /// </summary>
    class ColumnType<ColumnName, DataType> : Dictionary<string, string> {
        public ColumnType() : base() { }
        public ColumnType(int capacity) : base(capacity) { }
    }
}
