using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Balances;
using log4net.Config;

namespace Position
{
   public class Program
    {
        private static readonly log4net.ILog log = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod()?.DeclaringType);
        static void Main(string[] args)
        {
            log.Debug($"Position Started at: {DateTime.Now:dd-MM-yyyy H:mm:ss}");
            try
            {
                 DateTime date = DateTime.Parse(args[0]);
               // DateTime date = DateTime.Parse("2022/12/27");
                Console.WriteLine("Position reports running please wait...");
                Balance.WritePosition(date);
                Balance.ExportResultsTxt("0|SUCCESS");
                log.Info($"Job executed successfully at: {DateTime.Now:dd-MM-yyyy H:mm:ss}");
                Environment.Exit(0);
            }
            catch (Exception ex)
            {
                Balance.ExportResultsTxt("1|FAILURE");
                log.Error("Position Failed:\n " + ex.Message);
               
            }
        }
    }
}
