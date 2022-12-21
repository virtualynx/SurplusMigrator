using System;

namespace SurplusMigrator.Exceptions {
    internal class MissingDataException : Exception {
        public MissingDataException(string message) : base(message) {
        }
    }
}
