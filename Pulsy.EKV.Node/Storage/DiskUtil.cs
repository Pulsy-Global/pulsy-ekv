namespace Pulsy.EKV.Node.Storage;

internal static class DiskUtil
{
    public static long GetFreeBytes(string dataPath)
    {
        try
        {
            var fullPath = Path.GetFullPath(dataPath);
            if (!Directory.Exists(fullPath))
            {
                return 0;
            }

            var drive = new DriveInfo(fullPath);
            return drive.AvailableFreeSpace;
        }
        catch
        {
            return 0;
        }
    }
}
