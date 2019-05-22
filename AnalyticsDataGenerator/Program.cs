using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;

namespace db_connect
{
    class Program
    {
        const int StartingProjectSK = 1234;
        const int StartingBranchSK = 123456;

        static void Main(string[] args)
        {
            var sqlFormattedDate = 20190301;

            var files = Directory.EnumerateFiles(@"C:\VSO\src", "*.*", SearchOption.AllDirectories)
                .Where(file => new string[] { ".cs", ".tsx", ".ts" }
                .Contains(Path.GetExtension(file)))
                .Select(file => file.Remove(0, @"C:\VSO\".Length).Replace('\\', '/'))
                .ToList();

            Dictionary<string, long> durableSK = new Dictionary<string, long>();

            long fileSK = 0;

            foreach (var file in files)
            {
                durableSK.Add(file, fileSK++);
            }

            using (var fileStream = new StreamWriter(@"C:\AnalyticsPlayground\AzDevopsFilesMultipleBranches6MonthAccurate4.csv"))
            {
                fileStream.WriteLine("FileSK, DurableFileKey, BranchSK, ProjectSK, FullPath, RowEffectiveDate, RowExpiryDate, CurrentRow");
                fileStream.Flush();

                fileSK = 0;

                // First Insert All files for each active branch
                for (int i = 0; i < 10; ++i)
                {
                    foreach (var file in files)
                    {
                        var line = string.Format("{0},{1},{2},{3},{4},{5},{6},{7}",
                            fileSK++,
                            durableSK[file],
                            StartingBranchSK + i,
                            StartingProjectSK,
                            file,
                            sqlFormattedDate,
                            sqlFormattedDate + 1,
                            "Expired");

                        fileStream.WriteLine(line);
                        fileStream.Flush();
                    }
                }

                for (int day = 0; day <= 250; ++day)
                {
                    for (int build = 0; build <= 50; ++build)
                    {
                        for (int branch = 0; branch < 10; ++branch)
                        {
                            var randomSkip = new Random().Next(1, 90000);
                            var randomTake = 100;  

                            var resultSet = files.Skip(randomSkip).Take(randomTake);

                            foreach (var file in resultSet)
                            {
                                var line = string.Format("{0},{1},{2},{3},{4},{5},{6},{7}",
                                    fileSK++,
                                    durableSK[file],
                                    branch % 2 == 0 ? StartingBranchSK : StartingBranchSK + branch,
                                    StartingProjectSK,
                                    file + day + build + branch,
                                    sqlFormattedDate + day % 29 + (day/29) * 100,
                                    sqlFormattedDate + day % 29 + (day / 29) * 100 + 1,
                                    "Expired");

                                fileStream.WriteLine(line);
                                fileStream.Flush();
                            }
                        }
                    }
                }

                //for (int i = 0; i < 10; ++i)
                //{
                //    foreach (var file in files)
                //    {
                //        var line = string.Format("{0},{1},{2},{3},{4},{5},{6},{7}",
                //            fileSK++,
                //            durableSK[file],
                //            StartingBranchSK + i,
                //            StartingProjectSK,
                //            file,
                //            sqlFormattedDate + 181,
                //            99991231,
                //            "Current");

                //        fileStream.WriteLine(line);
                //        fileStream.Flush();
                //    }
                //}
            }
        }
    }
}
