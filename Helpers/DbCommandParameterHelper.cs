using System.Data;

namespace Platform.Messaging.Helpers;

public static class DbCommandParameterHelper
{
    public static void AddParameter(IDbCommand command, string name, object? value)
    {
        var parameter = command.CreateParameter();
        parameter.ParameterName = name;
        parameter.Value = value ?? DBNull.Value;
        command.Parameters.Add(parameter);
    }
}
