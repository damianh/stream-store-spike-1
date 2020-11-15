using System;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using App.Metrics;
using App.Metrics.Timer;
using Microsoft.Data.Sqlite;
using Xunit;
using Xunit.Abstractions;

namespace Spike1.Tests
{
    public class UnitTest1
    {
        private readonly ITestOutputHelper _outputHelper;
        private readonly IMetricsRoot _metrics;

        public UnitTest1(ITestOutputHelper outputHelper)
        {
            _outputHelper = outputHelper;

            _metrics = new MetricsBuilder()
                .Report.ToConsole()
                .Report.ToInfluxDb(options =>
                {
                    options.InfluxDb.BaseUri = new Uri("http://localhost:8086");
                    options.InfluxDb.Database = "sqlite-spike";
                    options.InfluxDb.UserName = "root";
                    options.InfluxDb.Password = "roor";
                })
                .Build();
        }

        [Fact]
        public async Task DatabaseSize()
        {
            var tempFileName = Path.GetTempFileName();
            var connectionStringBuilder = new SqliteConnectionStringBuilder
            {
                DataSource = tempFileName,
                Cache = SqliteCacheMode.Shared,
                Mode = SqliteOpenMode.ReadWriteCreate,
            };
            var connectionString = connectionStringBuilder.ConnectionString;

            var connection = new SqliteConnection(connectionString);

            await ExecuteNonQueryAsync(connection, CreateTables);

            var length = new FileInfo(tempFileName).Length;

            _outputHelper.WriteLine($"FileSize:{length} bytes");
            
            File.Delete(tempFileName);
        }

        [Fact]
        public async Task TimeToCreateDatabase()
        {
            var tempFileName = Path.GetTempFileName();
            var connectionStringBuilder = new SqliteConnectionStringBuilder
            {
                DataSource = tempFileName,
                Cache = SqliteCacheMode.Shared,
                Mode = SqliteOpenMode.ReadWriteCreate,
            };
            var connectionString = connectionStringBuilder.ConnectionString;

            var connection = new SqliteConnection(connectionString);

            var timerOptions = new TimerOptions
            {
                Name = "sqlite-db-create",
                MeasurementUnit = Unit.Commands,
                DurationUnit = TimeUnit.Milliseconds,
                RateUnit = TimeUnit.Milliseconds
            };

            for (int i = 0; i < 100; i++)
            {
                using (_metrics.Measure.Timer.Time(timerOptions))
                {
                    await ExecuteNonQueryAsync(connection, CreateTables);
                }
                File.Delete(tempFileName);
            }

            await Task.WhenAll(_metrics.ReportRunner.RunAllAsync());
        }

        [Fact]
        public async Task TimeToCreateManyDatabases()
        {
            var tempPath = Path.Combine(Path.GetTempPath(), "sqlite-spike", Guid.NewGuid().ToString());
            Directory.CreateDirectory(tempPath);
            var timerOptions = new TimerOptions
            {
                Name = "sqlite-db-create-many",
                MeasurementUnit = Unit.Commands,
                DurationUnit = TimeUnit.Milliseconds,
                RateUnit = TimeUnit.Milliseconds
            };

            for (int i = 0; i < 100; i++)
            {
                var tempFile = Path.Combine(tempPath, i.ToString());
                var connectionString = $"Data Source={tempFile};Cache=Shared;";
                var connection = new SqliteConnection(connectionString);
                
                using (_metrics.Measure.Timer.Time(timerOptions))
                {
                    await ExecuteNonQueryAsync(connection, CreateTables);
                }

                connection.Dispose();
            }

            var info = new DirectoryInfo(tempPath);
            var totalSize = (info.EnumerateFiles().Sum(file => file.Length)) / (1024 * 1024);
            _outputHelper.WriteLine($"Directory size:{totalSize}mb");

            await Task.WhenAll(_metrics.ReportRunner.RunAllAsync());

            Directory.Delete(tempPath, true);
        }

        [Fact]
        public async Task TimeToCreateSingleDB()
        {
            var timerOptionsStart = new TimerOptions
            {
                Name = "sqlite-provision",
                MeasurementUnit = Unit.Commands,
                DurationUnit = TimeUnit.Milliseconds,
                RateUnit = TimeUnit.Milliseconds
            };

            var tempFileName = "c:/dev/temp/spike1.db";

            var connectionString = $"Data Source={tempFileName};Cache=Shared;";

            var connection = new SqliteConnection(connectionString);

            _metrics.Measure.Timer.Time(timerOptionsStart);

            using (_metrics.Measure.Timer.Time(timerOptionsStart))
            {
                await ExecuteNonQueryAsync(connection, DropTables);

                await ExecuteNonQueryAsync(connection, CreateTables);
            }

            await Task.WhenAll(_metrics.ReportRunner.RunAllAsync());
        }

        [Theory]
        [InlineData(100)]
        [InlineData(1000)]
        [InlineData(10000)]
        [InlineData(100000)]
        [InlineData(1000000)]
        [InlineData(10000000)]
        public async Task BatchInsertAndSingleAppend(int insertCount)
        {
            var tempFileName = $"c:/dev/temp/sqlite-spike-batch-{insertCount}.db";

            if (File.Exists(tempFileName))
            {
                File.Delete(tempFileName);
            }

            var connectionStringBuilder = new SqliteConnectionStringBuilder
            {
                DataSource = tempFileName,
                Cache = SqliteCacheMode.Shared,
                Mode = SqliteOpenMode.ReadWriteCreate,
            };
            var connectionString = connectionStringBuilder.ConnectionString;
            var connection = new SqliteConnection(connectionString);
            await ExecuteNonQueryAsync(connection, CreateTables);

            var commandText = @"
INSERT INTO messages_2 (id, created)
VALUES (@id, @created)
";
            await connection.OpenAsync();
            using (var transaction = connection.BeginTransaction(IsolationLevel.Serializable))
            {
                await using var command = connection.CreateCommand();
                command.CommandText = commandText;

                var idParameter = command.CreateParameter();
                idParameter.ParameterName = "@id";
                command.Parameters.Add(idParameter);

                var createdParameter = command.CreateParameter();
                createdParameter.ParameterName = "@created";
                command.Parameters.Add(createdParameter);

                for (int i = 0; i < insertCount; i++)
                {
                    idParameter.Value = Guid.NewGuid().ToString();
                    createdParameter.Value = DateTime.UtcNow;

                    await command.ExecuteNonQueryAsync();
                }

                await transaction.CommitAsync();
            }
            {
                await using var command = connection.CreateCommand();
                command.CommandText = commandText;

                var idParameter = command.CreateParameter();
                idParameter.ParameterName = "@id";
                command.Parameters.Add(idParameter);

                var createdParameter = command.CreateParameter();
                createdParameter.ParameterName = "@created";
                command.Parameters.Add(createdParameter);

                var timerOptions = new TimerOptions
                {
                    Name = "sqlite-append-on-batch"
                };
                for (int i = 0; i < 25; i++)
                {
                    idParameter.Value = Guid.NewGuid().ToString();
                    createdParameter.Value = DateTime.UtcNow;

                    using (_metrics.Measure.Timer.Time(timerOptions))
                    {
                        await command.ExecuteNonQueryAsync();
                    }
                }
                await connection.CloseAsync();
            }

            WriteFileSize(tempFileName);


            await Task.WhenAll(_metrics.ReportRunner.RunAllAsync());
        }

        private async Task ExecuteNonQueryAsync(SqliteConnection connection, string commandText)
        {
            await using var command = connection.CreateCommand();

            await connection.OpenAsync();

            command.CommandText = commandText;

            await command.ExecuteNonQueryAsync();

            await connection.CloseAsync();
        }

        private void WriteFileSize(string path)
        {
            var length = new FileInfo(path).Length;

            var kbLength = length / 1024;

            var mbLength = kbLength / 1024;

            _outputHelper.WriteLine($"FileSize: {length} b, {kbLength} kb, {mbLength} mb");
        }

        private static string DropTables = @"
DROP TABLE IF EXISTS messages;

DROP TABLE IF EXISTS metadata;
";

        private static string CreateTables = @"
CREATE TABLE IF NOT EXISTS metadata(
    key         char(24)    NOT NULL PRIMARY KEY,
    value       text        NOT NULL
);

CREATE TABLE IF NOT EXISTS messages (
    id               char(36)    NOT NULL,
    version          integer     NOT NULL PRIMARY KEY AUTOINCREMENT,
    created          datetime    NOT NULL,
    UNIQUE (id)
);

CREATE TABLE IF NOT EXISTS messages_2 (
    id               char(36)    NOT NULL,
    version          integer     NOT NULL PRIMARY KEY AUTOINCREMENT,
    created          datetime    NOT NULL
);";
    }
}
