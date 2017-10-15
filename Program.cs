using System;
using System.Collections.Generic;
using System.Data.SQLite;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace super_broccoli
{
    class Program
    {
        private const string DATABASE_PATH = @"C:\Temp\test.sqlite";

        static SQLiteConnection GetConnection()
        {
            bool createTable = false;
            if (!File.Exists(DATABASE_PATH))
            {
                SQLiteConnection.CreateFile(DATABASE_PATH);
                createTable = true;
            }

            var conn = new SQLiteConnection($"Data Source={DATABASE_PATH};Connect Timeout={Int32.MaxValue}")
            {
                BusyTimeout = Int32.MaxValue,
                DefaultTimeout = Int32.MaxValue
            };

            conn.Open();

            if (createTable)
            {
                using (var command = new SQLiteCommand("CREATE TABLE test (guid text NOT NULL);", conn))
                {
                    command.ExecuteNonQuery();
                }
            }

            return conn;
        }

        static void Main(string[] args)
        {
            using (var tokenSource = new CancellationTokenSource())
            {
                Task.WaitAny(RunTasks(Environment.ProcessorCount, Environment.ProcessorCount, tokenSource.Token));
                tokenSource.Cancel();
                Log(">>> Tasks cancelled. Program paused.");
                Console.ReadKey();
            }
        }

        static async Task RunTasks(int readers, int writers, CancellationToken token)
        {
            var tasks = new List<Task>();

            // Task 1: Insert a bunch of records, then delete them (writer)
            for (int _ = 0; _ < writers; _++)
            {
                tasks.Add(new Task(() =>
                {
                    try
                    {
                        // We create a thread-local connection per SQLite documentation
                        // https://sqlite.org/threadsafe.html
                        var threadLocalConnection = GetConnection();
                        while (!token.IsCancellationRequested)
                        {
                            for (int i = 0; i < 1000; i++)
                            {
                                Log($"Inserting record {i}");
                                using (var command = new SQLiteCommand("INSERT INTO test VALUES('" + Guid.NewGuid().ToString() + "')", threadLocalConnection))
                                {
                                    command.ExecuteNonQuery();
                                }

                                if (token.IsCancellationRequested)
                                    break;
                            }

                            if (token.IsCancellationRequested)
                                break;

                            Log($"Resetting");
                            using (var command = new SQLiteCommand("DELETE FROM test; VACUUM;", threadLocalConnection))
                            {
                                command.ExecuteNonQuery();
                            }
                        }
                    }
                    catch(Exception e)
                    {
                        Log(e.ToString());
                    }
                }, token, TaskCreationOptions.LongRunning));
            }

            // Task 2: Query number of records in table (reader)
            for (int _ = 0; _ < readers; _++)
            {
                tasks.Add(new Task(() =>
                {
                    try
                    {
                        // We create a thread-local connection per SQLite documentation
                        // https://sqlite.org/threadsafe.html
                        var threadLocalConnection = GetConnection();
                        while (!token.IsCancellationRequested)
                        {
                            Log($"Reading records");
                            using (var command = new SQLiteCommand("SELECT COUNT(*) FROM test WHERE guid LIKE '%%'", threadLocalConnection))
                            {
                                Log("{0} records currently in table", (long)command.ExecuteScalar());
                            }
                        }
                    }
                    catch(Exception e)
                    {
                        Log(e.ToString());
                    }
                }, token, TaskCreationOptions.LongRunning));
            }

            Parallel.ForEach(tasks, task => task.Start());
            await Task.WhenAny(tasks);
        }

        private static void Log(string format, params object[] parameters)
        {
            var timestampedString = $"{Thread.CurrentThread.ManagedThreadId:D2}\t{DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss.fff")}\t" + format;
            Console.WriteLine(timestampedString, parameters);
        }
    }
}
