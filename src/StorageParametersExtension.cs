using System;
using Utility.Storage.StorageHelper.Common;

namespace Utility.Storage.StorageHelper
{
    static class StorageParametersExtension
    {
        public static DynamicParameters ToDapperParameters(this Func<Parameters, Parameters> getter)
        {
            if (getter == null) return null;

            var parameters = new Parameters();
            return (getter(parameters) ?? parameters).Result;
        }
    }
}
