using System;
using System.Collections.Generic;
using Microsoft.Spark.Sql;

namespace SparkProjects
{
    //https://www.youtube.com/watch?v=wy02ZodkVUo
    class Program
    {
        static void Main(string[] args)
        {
            var session = SparkSession.Builder().AppName("bank-analysis").GetOrCreate();
            session.SparkContext.SetLogLevel("ERROR");
            var df = session
                .Read()
                .Options(new Dictionary<string, string>
                {
                    {"header","true" },
                    {"inferSchema","true" },
                    {"delimiter",";" }
                })
                .Csv("an.csv");

            df.Show();
            df.PrintSchema();
            df.Describe().Show();

            Console.WriteLine($"Count - {df.Count()}");
            Console.WriteLine($"Count - {df.Count()}");

            var age = df.Select("age");
            age.Show();
            var multipleColumns = df.Select("age", "balance", "job");
            multipleColumns.Show();
            var nullValues = df.Count() - df.Na().Drop().Count();

            Console.WriteLine($"Null value count - {nullValues}");
            Console.WriteLine(Environment.NewLine);

            var balanceFilter = df.Filter(("balance < 0"));

            balanceFilter.Show();

            var balanceFilter2 = df.Filter(df["balance"]>0);

            balanceFilter2.Show();

            var jobGroup = df.GroupBy("job").Count();

            jobGroup.Show();

            var sort = df.Sort("balance");

            sort.Show();

            var dropDf = df.Drop("contact", "day", "month", "duration", "campaign", "pdays", "previous", "poutcome", "y", "housing", "loan");

            var renameDf = dropDf.WithColumnRenamed("default", "hasDefaulted").WithColumnRenamed("loan", "hasLoan");

            renameDf.Show();

            var valuesDf = renameDf.WithColumn("hasDefaulted",
                Functions.When(Functions.Col("hasDefaulted") == "y", 1).Otherwise(0));

            valuesDf.Show();

        }
    }
}
