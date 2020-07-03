using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading;
using System.Runtime.CompilerServices;

using ETM.WCCOA;

using Npgsql;
using static WCCOApostgree.Utils;

namespace GettingStarted
{
    // This C# program provides the same functionality as the C++ template manager.
    // it establishes connection to a project
    // reads necessary information from the config File if available
    // Connects on DPE1 and forwards value changes to DPE2

    class Program
    {
        static void Main(string[] args)
        {
            // Create Manager object
            OaManager myManager = OaSdk.CreateManager();

            // Initialize Manager Configuration
            myManager.Init(ManagerSettings.DefaultApiSettings, args);

            // Start the Manager and Connect to the OA project with the given configuration
            myManager.Start();
            
            // ReadString(section, key, defaultval)
            OaConfigurationFile file = new OaConfigurationFile();

            // get connection params from config
            string cfg_host  = file.ReadString("postgrees", "hostname", "localhost");
            string cfg_user  = file.ReadString("postgrees", "username", "postgres");
            string cfg_pass  = file.ReadString("postgrees", "password", "root");
            string cfg_dbase = file.ReadString("postgrees", "database", "wincc_data");
            
            Console.WriteLine("=============postgree manager init===============");
            Console.WriteLine("Hostname: " + cfg_host);
            Console.WriteLine("Database: " + cfg_dbase);
            Console.WriteLine("Username: " + cfg_user);
            Console.WriteLine("Password: " + cfg_pass);
            Console.WriteLine("=================================================");

            // configs from template
            string dpNameSet = file.ReadString("postgrees", "dpNameSet", "ExampleDP_Arg2.");
            string dpNameConnect = file.ReadString("postgrees", "dpNameConnect", "ExampleDP_Arg1.");

            // prepare connection string npgsql
            var connect_string = String.Format("Host={0};Username={1};Password={2};Database={3}", cfg_host, cfg_user, cfg_pass, cfg_dbase);
            NpgsqlConnection conn = new NpgsqlConnection(connect_string);
            try
            {
                conn.Open();
            }
            catch(Exception e)
            {
                Console.WriteLine("Error open connection database");
                Console.WriteLine("Connection string: " + connect_string);
                Console.WriteLine(e.Message);
                myManager.Stop();
            }

            // Get List datapoint to connect
            var valueAccess = myManager.ProcessValues;
            OaDpValueItem oaDpList = valueAccess.GetDpValue("_SYBCOM_HISTORY_LIST.dps");
            // convert datapoints str to list
            List<string> dps = strToList(oaDpList.DpValue.ToString(), '|');

            string wcc_query = "SELECT '_original.._value' FROM '{";
            string dba_query = "CALL public.new_data( @dp, @val)";

            foreach (var dp in dps)
            {
                if (dp != dps.Last<string>())
                    wcc_query += dp + ",";
                else
                    wcc_query += dp + "}'";
            }

            OaDpQuerySubscription hisQuerySubscription = valueAccess.CreateDpQuerySubscription();
            hisQuerySubscription.SetQuery(wcc_query);

            hisQuerySubscription.ValueChanged += (vcsender, vce) =>
            {
                var qRecCount = vce.Result.GetRecordCount();
                var qColCount = vce.Result.GetColumnCount();
                KeyValuePair<string, string>[] vals = new KeyValuePair<string, string>[qRecCount];
                string[] temp = new string[2];
                for(int i=0; i<qRecCount; i++)
                {
                    // vals[i] = new KeyValuePair<string, string>(vce.Result.GetData(i, 0), vce.Result.GetData(i, 1));

                    using (var cmd = new NpgsqlCommand(dba_query, conn))
                    {
                        cmd.Parameters.AddWithValue("dp", vce.Result.GetData(i, 0).ToString());
                        cmd.Parameters.AddWithValue("val", vce.Result.GetData(i, 1).ToString().Replace(',', '.'));
                        try
                        {
                            cmd.ExecuteNonQuery();
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine("Error query execute");
                            Console.WriteLine("Query string: " + dba_query);
                            Console.WriteLine("Params: ");
                            Console.WriteLine("DP: " + vce.Result.GetData(i, 0).ToString());
                            Console.WriteLine("VL: " + vce.Result.GetData(i, 1).ToString().Replace(',', '.'));
                            Console.WriteLine(e.Message);
                        }
                    }
                }
            };

            // hisQuerySubscription.FireChangedEventForAnswer = true;
            hisQuerySubscription.StartAsync();
            // ==============================================================================================================================
            // Create Subscription object
            var mySubscription = valueAccess.CreateDpValueSubscription();
             mySubscription.AddDpList("history", dps);
            // Append Datapoints to subcribe on
            // mySubscription.AddDp(dpNameConnect);

            // Define Lambda function for value changed event. Can be done as shon here as Lambda function or as seperate function
            mySubscription.SingleValueChanged += (vcsender, vce) =>
            {
                if (vce.Value == null)
                    return;
                if(vce.IsAnswer)
                {
                    Console.WriteLine("FIRST value: ");
                }
                else
                {
                    Console.WriteLine("Changed value: " + vce.Value.DpValue.ToString());
               //     foreach (var val in vce.Values)
               //     {
               //         string query = "CALL public.new_data( dp => @dp, val_dp => @val)";
               //         using (var cmd = new NpgsqlCommand(query, conn))
               //         {
               //             cmd.Parameters.AddWithValue("dp", val.DpName.ToString());
               //             cmd.Parameters.AddWithValue("val", val.DpValue.ToString());
               //             cmd.ExecuteNonQuery();
               //         }
               //     }
                }

                // Console.WriteLine("Received value: " + vce.Values.DpValue.ToString() + " for DPE: " + vce.Value.DpName.ToString());

                // vce.Value can be null in error case 
                /*
                if(vce.Value == null)
                  return;
                
                Console.WriteLine("Received value: " + vce.Value.DpValue.ToString() + " for DPE: " + vce.Value.DpName.ToString());

                conn.Open();
                using (var cmd = new NpgsqlCommand("INSERT INTO dp_1 (datetime, value) VALUES (CURRENT_TIMESTAMP, @val)", conn))
                {
                    cmd.Parameters.AddWithValue("val", vce.Value.DpValue.ToString());
                    cmd.ExecuteNonQuery();
                }
                conn.Close();

                Console.WriteLine("Added database: " + vce.Value.DpValue.ToString() + " for DPE: " + vce.Value.DpName.ToString());

                //Set received value on DPE dpNameSet
                valueAccess.SetDpValue(dpNameSet, vce.Value.DpValue.ToDouble());
                Console.WriteLine("Set value: " + vce.Value.DpValue.ToString() + " also on DPE: "+dpNameSet);
                */
            };

            // If FireChangedEventForAnswer is set to true, the ValueChanged Event is alsed fired for the first answer
            mySubscription.FireChangedEventForAnswer = true;

            // Start the subscription and as an additional option wait for the first anwer as result value
            // mySubscription.StartAsync();

        }
    }
}