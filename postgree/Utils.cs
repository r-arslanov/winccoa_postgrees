using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using ETM.WCCOA;


namespace WCCOApostgree
{
    class Utils
    {
        // WCCOA string array to C# list variable
        public static List<string> strToList(string wcc_array, char delim) {
            List<string> tmpList = new List<string>(wcc_array.Split(delim));
            List<string> resList = new List<string> { };
            foreach (var dp in tmpList)
            {
                resList.Add(dp.Trim(' '));
            }
            return resList;
        }
    }
}
