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

            var path = "float_timeseries_1.parquet";

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

            var path = "float_timeseries_2.parquet";

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

            var path = "float_timeseries_3.parquet";

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
    }
}
