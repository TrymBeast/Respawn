﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Respawn
{
    internal class DatabaseTables
    {
        public string[] TablesToDelete { get; set; }
        public string[] TablesToDisableFKContraints { get; set; }
    }
}
