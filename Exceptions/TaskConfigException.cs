using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SurplusMigrator.Exceptions {
    internal class TaskConfigException : Exception {
        public TaskConfigException(string message) : base(message) {
        }
    }
}
