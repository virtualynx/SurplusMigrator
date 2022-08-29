using System;

namespace SurplusMigrator.Exceptions {
    internal class TaskConfigException : Exception {
        public TaskConfigException(string message) : base(message) {
        }
    }
}
