namespace Pulsy.EKV.Node.Models;

internal static class NamespaceNames
{
    public const int MaxLength = 255;

    public const string ValidationMessage =
        "namespace must be 1-255 characters and contain only letters, digits, '.', '_' or '-'";

    public static bool IsValid(string? namespaceName)
    {
        if (string.IsNullOrWhiteSpace(namespaceName) || namespaceName.Length > MaxLength)
        {
            return false;
        }

        if (namespaceName is "." or "..")
        {
            return false;
        }

        foreach (var ch in namespaceName)
        {
            if (!IsAllowed(ch))
            {
                return false;
            }
        }

        return true;
    }

    public static void ThrowIfInvalid(string? namespaceName, string paramName)
    {
        if (!IsValid(namespaceName))
        {
            throw new ArgumentException(ValidationMessage, paramName);
        }
    }

    private static bool IsAllowed(char ch)
        => ch is >= 'a' and <= 'z'
            or >= 'A' and <= 'Z'
            or >= '0' and <= '9'
            or '.'
            or '_'
            or '-';
}
