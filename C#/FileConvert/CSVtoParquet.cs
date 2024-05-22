// Just a quick note: Please remember to replace "CSV FILE PATH" with the path to your CSV file and "OUTPUT file path(MUST be in .parquet format like hello.parquet)" 
// with the path where you want to save the Parquet file.

// Also, don’t forget to install the ChoETL.Parquet NuGet package before running the code. 
// You can do this by running the following command in the terminal: Install-Package ChoETL.Parquet 
// (install ChoETL using this console command in terminal: install-package ChoETL.Parquet)

// // Note: use this link to open Parquet File https://www.parquet-viewer.com/#parquet-online

// program done in 2 ways (when you run/use method 1 then comment 2nd method)
// **************Method 1: *****************

// it contains 3 methods for write in file.
using ChoETL;
using System.Data;
class Program
{
    static void Main(string[] args)
    {
        Console.WriteLine("Hello World!");
        Console.WriteLine("Start time: " + DateTime.Now);
        
        //convert CSV to.Net Datatable
        DataTable dt = new DataTable();
        using (StreamReader sr = new StreamReader(@"CSV FILE PATH"))
        {
            string[] headers = sr.ReadLine().Split(',');
            foreach (string header in headers)
            {
                dt.Columns.Add(header);
            }
            while (!sr.EndOfStream)
            {
                string[] rows = sr.ReadLine().Split(',');
                DataRow dr = dt.NewRow();
                for (int i = 0; i < headers.Length; i++)
                {
                    dr[i] = rows[i];
                }
                dt.Rows.Add(dr);
            }
        }
        //Write DATATBLE to PARQUET file format 3 methods
        //  Method 1 : direct write data from datatable
        using (var w = new ChoParquetWriter("OUTPUT file path"))
        {
            w.Write(dt);
        }

        //  Method 2 : It will write data in chunks and creates multiple files with chunk data (No. of files = no. of chunks)

        //int chunkSize = 1000;
        //int totalChunks = (int)Math.Ceiling((double)dt.Rows.Count / chunkSize);

        //Parallel.For(0, totalChunks, chunk =>
        //{
        //    var chunkRows = dt.AsEnumerable().Skip(chunk * chunkSize).Take(chunkSize).CopyToDataTable();
        /// **** here you have to mention folder location before "/hello2_{chunk}.parquet"
        //    using (var w = new ChoParquetWriter($"OUTPUT file path/hello2_{chunk}.parquet"))    
        //    {
        //        w.Write(chunkRows);
        //    }
        //});

        // Method 3 : write data in single file with chunks

        //int chunkSize = 1000;
        //using (var w = new ChoParquetWriter("OUTPUT file path"))
        //{
        //    Parallel.For(0, (int)Math.Ceiling((double)dt.Rows.Count / chunkSize), chunk =>
        //    {
        //        var chunkRows = dt.AsEnumerable().Skip(chunk * chunkSize).Take(chunkSize).CopyToDataTable();
        //        lock (w)
        //        {
        //            w.Write(chunkRows);
        //        }
        //    });
        //}

        Console.WriteLine("End Time: " + DateTime.Now);
    }
}


//   ********      -------------------------------------

// **************Method 2: *****************

using ChoETL;
using System.Collections;

namespace csvtoparquet
{
    class Program
    {
        public class CustomFileReader : IEnumerable<List<string>>, IDisposable
        {
            StreamReader sr;
            int _batchSize = 1;

            public CustomFileReader(string path, int batchSize)
            {
                if (batchSize > 0)
                    _batchSize = batchSize;
                else
                    throw new ArgumentException("Batch size should be greater than Zero", "batchSize");

                sr = File.OpenText(path);
            }

            public void Dispose()
            {
                if (sr != null)
                {
                    sr.Close();
                }
            }

            public IEnumerator<List<string>> GetEnumerator()
            {
                string input = string.Empty;

                while (!sr.EndOfStream)
                {
                    int i = 0;

                    List<string> batch = new List<string>();

                    while (i < _batchSize && !string.IsNullOrEmpty((input = sr.ReadLine())))
                    {
                        batch.Add(input);
                        i++;
                    }

                    if (batch.Count != 0)
                    {
                        yield return batch;
                    }
                }
                Dispose();
            }

            IEnumerator IEnumerable.GetEnumerator()
            {
                return GetEnumerator();
            }
        }

        public static async Task Main(string[] args)
        {
            Console.WriteLine("Start Time: " + DateTime.Now);
            string filePath_out = "OUTPUT file path";
            int batch_size = 10000;
            CustomFileReader reader = new CustomFileReader("CSV FILE PATH", batch_size);

            using (var writer = new ChoParquetWriter(filePath_out, new ChoParquetRecordConfiguration { CompressionMethod = Parquet.CompressionMethod.Snappy })
                     .Configure(c => c.FieldValueTrimOption = ChoFieldValueTrimOption.None)
                     .Configure(c => ChoParquetRecordConfiguration.LiteParsing = true)
                     .Configure(c => c.RowGroupSize = 5000))
            {
                await Task.Run(() =>
                {
                    Parallel.ForEach(reader, batch =>
                    {
                        var names = batch;
                        var joinedNames = new System.Text.StringBuilder();
                        names.ForEach(a => joinedNames.Append((joinedNames.Length > 0 ? "\n" : "") + a));

                        using (var r = ChoCSVReader.LoadText(joinedNames.ToString())
                                .Configure(c => c.NullValueHandling = ChoNullValueHandling.Empty)
                                .WithMaxScanRows(1000))
                        {
                            lock (writer)
                            {
                                writer.Write(r);
                            }
                        }
                    });
                });
            }
            Console.WriteLine("End Time: " + DateTime.Now);
        }
    }
}
