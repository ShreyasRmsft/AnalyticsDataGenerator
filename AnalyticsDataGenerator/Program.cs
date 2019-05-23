using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;

namespace db_connect
{
    class Program
    {
        const int StartingBuildPipelineSK = 123;
        const int StartingProjectSK = 1234;
        const int StartingBranchSK = 123456;

        const int NumberOfBranches = 3;
        const int DaysToGenerateDataFor = 30;
        const int NumberOfBuidsPerDay = 30;
        const int NumberOfFilesRenamedPerBuild = 10;

        static int numberOfFilesLimit = -1;

        static Random RandomNumberGenerator = new Random();

        static void Main(string[] args)
        {
            var files = Directory.EnumerateFiles(@"C:\VSO\src", "*.*", SearchOption.AllDirectories)
                .Where(file => new string[] { ".cs", ".tsx", ".ts" }
                .Contains(Path.GetExtension(file)))
                .Select(file => file.Remove(0, @"C:\VSO\".Length).Replace('\\', '/'))
                .ToList();

            if (numberOfFilesLimit != -1)
            {
                files = files.Take(numberOfFilesLimit).ToList();
            }

            Dictionary <long, Dictionary<string, long>> durableSK = new Dictionary<long, Dictionary<string, long>>();
            Dictionary<long, Dictionary<string, long>> lastKnownSk = new Dictionary<long, Dictionary<string, long>>();

            long durableFileSK = 0;

            for (int currentBranchIndex = 0; currentBranchIndex < NumberOfBranches; currentBranchIndex++)
            {
                durableSK[currentBranchIndex] = new Dictionary<string, long>();
                lastKnownSk[currentBranchIndex] = new Dictionary<string, long>();
                foreach (var file in files)
                {
                    durableSK[currentBranchIndex].Add(file, durableFileSK++);
                }
            }

            using (var factFileStream = new StreamWriter(Path.Combine(Directory.GetCurrentDirectory(), $"Fact{DateTime.Now.ToString("yyyyMMdd-hhmmss")}.csv")))
            {
                factFileStream.WriteLine("FileSK, BranchSK, ProjectSK, BuildPipelineSK, DateSK, BuildId, TotalLines, CoveredLines");
                var currentBuildId = 0;

                using (var dimensionFileStream = new StreamWriter(Path.Combine(Directory.GetCurrentDirectory(), $"FileDimension{DateTime.Now.ToString("yyyyMMdd-hhmmss")}.csv")))
                {
                    dimensionFileStream.WriteLine("FileSK, DurableFileKey, BranchSK, ProjectSK, FullPath, RowEffectiveDate, RowExpiryDate, CurrentRow");
                    dimensionFileStream.Flush();

                    var currentfileSK = 0;
                    var startingDate = DateTime.Parse("2019-01-01");

                    // First Insert All files for each active branch
                    for (int currentBranchIndex = 0; currentBranchIndex < NumberOfBranches; currentBranchIndex++)
                    {
                        currentBuildId++;

                        foreach (var file in files)
                        {
                            lastKnownSk[currentBranchIndex][file] = currentfileSK;

                            var dimLine = string.Format("{0},{1},{2},{3},{4},{5},{6},{7}",
                                currentfileSK,
                                durableSK[currentBranchIndex][file],
                                StartingBranchSK + currentBranchIndex,
                                StartingProjectSK,
                                file,
                                startingDate.ToString("yyyyMMdd"),
                                startingDate.AddDays(1).ToString("yyyyMMdd"),
                                "Expired");

                            dimensionFileStream.WriteLine(dimLine);
                            dimensionFileStream.Flush();

                            var totalLines = RandomNumberGenerator.Next(10, 200);
                            var coveredLines = RandomNumberGenerator.Next(0, totalLines);

                            var factLine = string.Format("{0},{1},{2},{3},{4},{5},{6},{7}",
                                currentfileSK,
                                StartingBranchSK + currentBranchIndex,
                                StartingProjectSK,
                                StartingBuildPipelineSK,
                                startingDate.ToString("yyyyMMdd"),
                                currentBuildId,
                                totalLines,
                                coveredLines);

                            factFileStream.WriteLine(factLine);
                            factFileStream.Flush();

                            currentfileSK++;
                        }
                    }

                    for (int currentDay = 1; currentDay <= DaysToGenerateDataFor; currentDay++)
                    {
                        for (int currentBranchIndex = 0; currentBranchIndex < NumberOfBranches; ++currentBranchIndex)
                        {
                            for (int buildIndex = 0; buildIndex <= NumberOfBuidsPerDay; buildIndex++)
                            {
                                currentBuildId++;

                                var randomSkip = RandomNumberGenerator.Next(0, files.Count - NumberOfFilesRenamedPerBuild);
                                var randomTake = NumberOfFilesRenamedPerBuild;

                                var resultSet = files.Skip(randomSkip).Take(randomTake);

                                foreach (var file in resultSet)
                                {
                                    lastKnownSk[currentBranchIndex][file] = currentfileSK;

                                    var line = string.Format("{0},{1},{2},{3},{4},{5},{6},{7}",
                                        currentfileSK,
                                        durableSK[currentBranchIndex][file],
                                        StartingBranchSK + currentBranchIndex,
                                        StartingProjectSK,
                                        file + "*" + startingDate.AddDays(currentDay).ToString("yyyyMMdd") + "-" + buildIndex,
                                        startingDate.AddDays(currentDay).ToString("yyyyMMdd"),
                                        startingDate.AddDays(currentDay + 1).ToString("yyyyMMdd"),
                                        "Expired");

                                    dimensionFileStream.WriteLine(line);
                                    dimensionFileStream.Flush();

                                    var totalLines = RandomNumberGenerator.Next(10, 200);
                                    var coveredLines = RandomNumberGenerator.Next(0, totalLines);

                                    var factLine = string.Format("{0},{1},{2},{3},{4},{5},{6},{7}",
                                        currentfileSK,
                                        StartingBranchSK + currentBranchIndex,
                                        StartingProjectSK,
                                        StartingBuildPipelineSK,
                                        startingDate.AddDays(currentDay).ToString("yyyyMMdd"),
                                        currentBuildId,
                                        totalLines,
                                        coveredLines);

                                    factFileStream.WriteLine(factLine);
                                    factFileStream.Flush();

                                    currentfileSK++;
                                }

                                foreach (var file in files)
                                {
                                    if (resultSet.Contains(file))
                                    {
                                        continue;
                                    }

                                    var totalLines = RandomNumberGenerator.Next(10, 200);
                                    var coveredLines = RandomNumberGenerator.Next(0, totalLines);

                                    var factLine = string.Format("{0},{1},{2},{3},{4},{5},{6},{7}",
                                        lastKnownSk[currentBranchIndex][file],
                                        StartingBranchSK + currentBranchIndex,
                                        StartingProjectSK,
                                        StartingBuildPipelineSK,
                                        startingDate.ToString("yyyyMMdd"),
                                        currentBuildId,
                                        totalLines,
                                        coveredLines);

                                    factFileStream.WriteLine(factLine);
                                    factFileStream.Flush();
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
                    //}
                }
            }
        }
    }
}
