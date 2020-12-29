using System;
using System.Collections.Generic;
using Apache.Arrow;
using Microsoft.Spark.Sql;

namespace GettingData
{
    class Program
    {
        static void Main(string[] args)
        {
            //var sparkSession = SparkSession.Builder().GetOrCreate();

            //var options =  new Dictionary<string, string>
            //{
            //    {"delimeter","|" },
            //    {"inferSchema","true" },
            //    {"samplingRation","5.0" },
            //};

            //var schemaString = "invDate INT, invItemId INT, invWareHouseId INT, invQuantityOnHand STRING";
            //var csvFile = sparkSession.Read()
            //    .Format("csv")
            //    .Options(options)
            //    .Schema(schemaString)
            //    .Load(@"D:\databases\dat\one_gb\inventory.dat");

            //csvFile.PrintSchema();
            //csvFile.Show(5);

            var sparkSession = SparkSession.Builder().GetOrCreate();
            var schemaString = "invDate INT, invItemId INT, invWareHouseId INT, invQuantityOnHand STRING";

            var csvFile = sparkSession.Read()
                .Format("csv")
                .Option("header", true)
                .Schema(schemaString)
                .Load(@"D:\databases\dat\one_gb\inventory.dat");
            csvFile.PrintSchema();
            csvFile.Show();
        }
    }
}
