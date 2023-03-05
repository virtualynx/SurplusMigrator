namespace SurplusMigrator.Models {
    class TaskTruncateOption {
        public bool truncateBeforeInsert { get; set; } = false;
        public bool onlyTruncateMigratedData { get; set; } = true;
        public bool cascade { get; set; } = false;
    }
}
