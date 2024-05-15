using ChoETL;
using System.Data;

    //  for write parquet format  -->> install ChoETL  using this console command in terminal:  install-package ChoETL.Parquet
    // *** Note *** : use this link to open Parquet File   https://www.parquet-viewer.com/#parquet-online
class Program
{
    static void Main(string[] args)
    {
        Console.WriteLine("Hello World!");
        Console.WriteLine("Start time: " + DateTime.Now);

        // convert CSV to .Net Datatable
        DataTable dt = new DataTable();
        using (StreamReader sr = new StreamReader(@"C:\Jinesh\Practice\CSVtoParquet\CSVtoParquet\demo.csv"))
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

        //write datatble to PARQUET file format
        using (var w = new ChoParquetWriter("C://Jinesh/Practice/CSVtoParquet/CSVtoParquet/hello2.parquet"))
        {
            w.Write(dt);
        }
        Console.WriteLine("End Time: "+DateTime.Now);
    }
}