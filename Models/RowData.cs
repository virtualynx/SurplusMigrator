using System;
using System.Collections.Generic;

namespace SurplusMigrator.Models
{
    class RowData<String, Object> : Dictionary<string, object> {
        public RowData() : base() { }
        public RowData(int capacity) : base(capacity) { }
    }
}
