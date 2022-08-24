using System.Collections.Generic;

namespace SurplusMigrator.Models {
    class TaskInsertStatus {
        public List<DbInsertFail> errors { get; set; } = new List<DbInsertFail>();
        public long successCount = 0;
    }
}
