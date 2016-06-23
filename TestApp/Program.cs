using System;
using Renci.SshNet;
using System.IO;
using System.Text.RegularExpressions;

namespace TestApp
{
    class Program
    {
        static void Main(string[] args)
        {
            const int port = 57322;
            const string host = "79.170.167.30";
            const string username = "user3";
            const string password = "userIntelPhi3";
            const string workingdirectory = "/home/user3/shamanov.i/JavaSparkAirlineDelay/";
            const string uploadfile = @"C:\Users\shama_000\Desktop\Университет\TERM_10\ТП\airlines_data_last_1_month.csv";

            // Upload data file
            Console.WriteLine("Creating client and connecting");
            using (var sftpclient = new SftpClient(host, port, username, password))
            {
                sftpclient.Connect();
                Console.WriteLine("Connected to {0}", host);

                sftpclient.ChangeDirectory(workingdirectory);
                Console.WriteLine("Changed directory to {0}", workingdirectory);

                var listDirectory = sftpclient.ListDirectory(workingdirectory);
                Console.WriteLine("Listing directory:");
                foreach (var fi in listDirectory)
                {
                    Console.WriteLine(" - " + fi.Name);
                }

                using (var fileStream = new FileStream(uploadfile, FileMode.Open))
                {
                    Console.WriteLine("Uploading {0} ({1:N0} bytes)", uploadfile, fileStream.Length);
                    sftpclient.BufferSize = 4 * 1024; // bypass Payload error large files
                    sftpclient.UploadFile(fileStream, Path.GetFileName(uploadfile));
                }
            }

            // Execute a (SHELL) Commands 
            using (var sshclient = new SshClient(host, port, username, password))
            {
                string result;
                sshclient.Connect();

                using (var cmd = sshclient.CreateCommand("ssh node23 'cd ~/shamanov.i/JavaSparkAirlineDelay/ && hadoop fs -copyFromLocal -f " + Path.GetFileName(uploadfile) + " /user/user3/shamanov.i/JavaSparkAirlineDelay'"))
                {
                    cmd.Execute();
                    Console.WriteLine("Command>" + cmd.CommandText);
                    Console.WriteLine("Return Value = {0}", cmd.ExitStatus);
                }

                using (var cmd = sshclient.CreateCommand("ssh node23 'cd ~/shamanov.i/JavaSparkAirlineDelay/ && spark-submit --class JavaSparkAirlineDelay --master yarn --deploy-mode cluster --executor-memory 1024m --num-executors 2 JavaSparkAirlineDelay.jar shamanov.i/JavaSparkAirlineDelay/" + Path.GetFileName(uploadfile) + "'"))
                {
                    cmd.Execute();
                    Console.WriteLine("Command>" + cmd.CommandText);
                    Console.WriteLine("Return Value = {0}", cmd.ExitStatus);

                    var reader = new StreamReader(cmd.ExtendedOutputStream);
                    result = reader.ReadToEnd();
                }

                Regex pattern = new Regex(@"tracking URL:.*proxy\/(?<runId>[^\/]+)\/");
                Match match = pattern.Match(result);
                string runId = match.Groups["runId"].Value;

                using (var cmd = sshclient.CreateCommand("ssh node23 'cd ~/shamanov.i/JavaSparkAirlineDelay/logs && yarn logs -applicationId " + runId + " > " + runId + ".log'"))
                {
                    cmd.Execute();
                    Console.WriteLine("Command>" + cmd.CommandText);
                    Console.WriteLine("Return Value = {0}", cmd.ExitStatus);
                }

                using (var cmd = sshclient.CreateCommand("ssh node23 'cd ~/shamanov.i/JavaSparkAirlineDelay/logs && cat " + runId + ".log'"))
                {
                    cmd.Execute();
                    Console.WriteLine("Command>" + cmd.CommandText);
                    Console.WriteLine("Return Value = {0}", cmd.ExitStatus);
                    
                    Console.Write(cmd.Result);
                }

                sshclient.Disconnect();
            }

            System.Console.ReadKey();
        }
    }
}
