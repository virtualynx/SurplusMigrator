using System;

namespace SurplusMigrator.Exceptions {
    internal class JobAbortedException : Exception {
        public JobAbortedException() : base() {
        }
        public JobAbortedException(string message) : base(message) {
        }
    }
}
