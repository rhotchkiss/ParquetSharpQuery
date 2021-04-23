using ParquetSharp.RowOriented;
using System;
using System.IO;
using Xunit;

namespace ParquetSharpQuery
{
    public class UnitTest1
    {
        [Fact]
        public void TestReadWrite_1Row()
        {
            var records = new (DateTime, double)[]{
                    (new DateTime(2019,1,1, 1, 1,0), 1.0),};

            var path = "TestReadWrite_1Row.parquet";

            using (var rowWriter = ParquetFile.CreateRowWriter<(DateTime, double)>(path))
                rowWriter.WriteRows(records);

            using (var rowReader = ParquetFile.CreateRowReader<(DateTime, double)>(path))
            {
                Assert.Equal(1, rowReader.FileMetaData.NumRowGroups);
                var read = rowReader.ReadRows(0);
                Assert.Equal<(DateTime, double)>(records, read);
            }

            File.Delete(path);
        }

        [Fact]
        public void TestReadWrite_2Rows()
        {
            var records = new (DateTime, double)[]{
                    (new DateTime(2019,1,1, 1, 1,0), 1.0),
                    (new DateTime(2019,1,1, 1, 2,0), 1.1),};

            var path = "TestReadWrite_2Rows.parquet";

            using (var rowWriter = ParquetFile.CreateRowWriter<(DateTime, double)>(path))
                rowWriter.WriteRows(records);

            using (var rowReader = ParquetFile.CreateRowReader<(DateTime, double)>(path))
            {
                Assert.Equal(1, rowReader.FileMetaData.NumRowGroups);
                var read = rowReader.ReadRows(0);
                Assert.Equal<(DateTime, double)>(records, read);
            }

            File.Delete(path);
        }
        
        [Fact]
        public void TestReadWrite_3Rows()
        {
            var records = new (DateTime, double)[]{(new DateTime(2019,1,1, 1, 1,0), 1.0),
                    (new DateTime(2019,1,1, 1, 2,0), 1.1),
                    (new DateTime(2019,1,1, 1, 3,0), 1.2),};

            var path = "TestReadWrite_3Rows.parquet";

            using (var rowWriter = ParquetFile.CreateRowWriter<(DateTime, double)>(path))
                rowWriter.WriteRows(records);

            using (var rowReader = ParquetFile.CreateRowReader<(DateTime, double)>(path))
            {
                Assert.Equal(1, rowReader.FileMetaData.NumRowGroups);
                var read = rowReader.ReadRows(0);
                Assert.Equal<(DateTime, double)>(records, read);
            }

            File.Delete(path);
        }

        [Fact]
        public void TestReadWrite_Doubles()
        {
            var records = new (double, double)[]{ 
                (1.0,1.1),
                (1.2,1.3),
                (1.0,1.1),
                (1.2,1.3),
                (1.0,1.1),
                (1.2,1.3),
                (1.0,1.1),
                (1.2,1.3),
                (1.0,1.1),
                (1.2,1.3),
                (1.0,1.1),
                (1.2,1.3),
                (1.0,1.1),
                (1.2,1.3),
            };

            var path = "TestReadWrite_Doubles.parquet";

            using (var rowWriter = ParquetFile.CreateRowWriter<(double, double)>(path))
                rowWriter.WriteRows(records);

            using (var rowReader = ParquetFile.CreateRowReader<(double, double)>(path))
            {
                Assert.Equal(1, rowReader.FileMetaData.NumRowGroups);
                var read = rowReader.ReadRows(0);
                Assert.Equal<(double, double)>(records, read);
            }

            File.Delete(path);
        }

        [Fact]
        public void TestReadWrite_DateTimes()
        {
            var records = new (DateTime, DateTime)[]{
                (new DateTime(2019,1,1, 1, 2,0),new DateTime(2019,1,1, 1, 2,0)),
                (new DateTime(2019,1,1, 1, 2,0),new DateTime(2019,1,1, 1, 2,0)),
                (new DateTime(2019,1,1, 1, 2,0),new DateTime(2019,1,1, 1, 2,0)),
                (new DateTime(2019,1,1, 1, 2,0),new DateTime(2019,1,1, 1, 2,0)),
                (new DateTime(2019,1,1, 1, 2,0),new DateTime(2019,1,1, 1, 2,0)),
                (new DateTime(2019,1,1, 1, 2,0),new DateTime(2019,1,1, 1, 2,0)),
                (new DateTime(2019,1,1, 1, 2,0),new DateTime(2019,1,1, 1, 2,0)),
                (new DateTime(2019,1,1, 1, 2,0),new DateTime(2019,1,1, 1, 2,0)),
                (new DateTime(2019,1,1, 1, 2,0),new DateTime(2019,1,1, 1, 2,0)),
                (new DateTime(2019,1,1, 1, 2,0),new DateTime(2019,1,1, 1, 2,0)),
            };

            var path = "TestReadWrite_DateTimes.parquet";

            using (var rowWriter = ParquetFile.CreateRowWriter<(DateTime, DateTime)>(path))
                rowWriter.WriteRows(records);

            using (var rowReader = ParquetFile.CreateRowReader<(DateTime, DateTime)>(path))
            {
                Assert.Equal(1, rowReader.FileMetaData.NumRowGroups);
                var read = rowReader.ReadRows(0);
                Assert.Equal<(DateTime, DateTime)>(records, read);
            }

            File.Delete(path);
        }
    }
}
