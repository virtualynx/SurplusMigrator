using System;
using System.Collections.Generic;
using System.Diagnostics;

namespace SurplusMigrator.Models {
    class TaskInsertStatus {
        public List<DbInsertFail> failures { get; set; } = new List<DbInsertFail>();
        public long successCount = 0;
    }
}
