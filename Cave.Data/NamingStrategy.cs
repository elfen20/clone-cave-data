using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Reflection;
using Cave.IO;

namespace Cave.Data
{
    public enum NamingStrategy
    {
        Exact,
        CamelCase,
        SnakeCase,
    }
}
