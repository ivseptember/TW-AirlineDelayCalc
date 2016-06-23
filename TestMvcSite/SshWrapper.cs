using System;
using Renci.SshNet;
using System.IO;
using System.Text.RegularExpressions;
using System.Text;

namespace TestMvcSite
{
    public class SshWrapper
    {
        static int port = 57322;
        static string host = "79.170.167.30";
        static string username = "user3";
        static string password = "userIntelPhi3";
        static string workingdirectory = "/home/user3/shamanov.i/JavaSparkAirlineDelay/";

        public static string ProcessTask(string uploadfile, int executorcount)
        {
            StringBuilder resBuilder = new StringBuilder();

            // Upload data file
            UploadFile(uploadfile, resBuilder);

            // Execute a (SHELL) Commands 
            RunDelaysCalculation(uploadfile, resBuilder);

            return resBuilder.ToString();
        }

        private static void UploadFile(string uploadfile, StringBuilder resBuilder)
        {
            resBuilder.AppendLine("Creating client and connecting");
            using (var sftpclient = new SftpClient(host, port, username, password))
            {
                sftpclient.Connect();
                resBuilder.AppendFormat("Connected to {0}\n", host);

                sftpclient.ChangeDirectory(workingdirectory);
                resBuilder.AppendFormat("Changed directory to {0}\n", workingdirectory);

                using (var fileStream = new FileStream(uploadfile, FileMode.Open))
                {
                    resBuilder.AppendFormat("Uploading {0} ({1:N0} bytes)\n\n", uploadfile, fileStream.Length);
                    sftpclient.BufferSize = 4 * 1024; // bypass Payload error large files
                    sftpclient.UploadFile(fileStream, Path.GetFileName(uploadfile));
                }
            }
        }

        private static void RunDelaysCalculation(string uploadfile, StringBuilder resBuilder)
        {
            using (var sshclient = new SshClient(host, port, username, password))
            {
                int retValue = 0;
                string runId = "";
                string result = "";

                sshclient.Connect();
                retValue = LoadDataToHDFS(uploadfile, resBuilder, sshclient);

                if (retValue == 0)
                {
                    CalculateDelays(uploadfile, resBuilder, sshclient, out retValue, out result);
                }

                if (retValue == 0)
                {
                    GetLogsFromYarn(resBuilder, sshclient, out retValue, out runId, result);
                }

                if (retValue == 0)
                {
                    LoadResults(resBuilder, sshclient, runId);
                }

                sshclient.Disconnect();
            }
        }

        private static int LoadDataToHDFS(string uploadfile, StringBuilder resBuilder, SshClient sshclient)
        {
            int retValue;
            using (var cmd = sshclient.CreateCommand("ssh node23 'cd ~/shamanov.i/JavaSparkAirlineDelay/ && hadoop fs -copyFromLocal -f " + Path.GetFileName(uploadfile) + " /user/user3/shamanov.i/JavaSparkAirlineDelay'"))
            {
                cmd.Execute();
                resBuilder.AppendLine("Command>" + cmd.CommandText);
                resBuilder.AppendFormat("Return Value = {0}\n\n", cmd.ExitStatus);

                retValue = cmd.ExitStatus;
            }

            return retValue;
        }

        private static void CalculateDelays(string uploadfile, StringBuilder resBuilder, SshClient sshclient, out int retValue, out string result)
        {
            using (var cmd = sshclient.CreateCommand("ssh node23 'cd ~/shamanov.i/JavaSparkAirlineDelay/ && spark-submit --class JavaSparkAirlineDelay --master yarn --deploy-mode cluster --executor-memory 1024m --num-executors 2 JavaSparkAirlineDelay.jar shamanov.i/JavaSparkAirlineDelay/" + Path.GetFileName(uploadfile) + "'"))
            {
                cmd.Execute();
                resBuilder.AppendLine("Command>" + cmd.CommandText);
                resBuilder.AppendFormat("Return Value = {0}\n\n", cmd.ExitStatus);

                var reader = new StreamReader(cmd.ExtendedOutputStream);
                result = reader.ReadToEnd();

                retValue = cmd.ExitStatus;
            }
        }

        private static void GetLogsFromYarn(StringBuilder resBuilder, SshClient sshclient, out int retValue, out string runId, string result)
        {
            Regex pattern = new Regex(@"tracking URL:.*proxy\/(?<runId>[^\/]+)\/");
            Match match = pattern.Match(result);
            runId = match.Groups["runId"].Value;

            using (var cmd = sshclient.CreateCommand("ssh node23 'cd ~/shamanov.i/JavaSparkAirlineDelay/logs && yarn logs -applicationId " + runId + " > " + runId + ".log'"))
            {
                cmd.Execute();
                resBuilder.AppendLine("Command>" + cmd.CommandText);
                resBuilder.AppendFormat("Return Value = {0}\n\n", cmd.ExitStatus);

                retValue = cmd.ExitStatus;
            }
        }

        private static void LoadResults(StringBuilder resBuilder, SshClient sshclient, string runId)
        {
            using (var cmd = sshclient.CreateCommand("ssh node23 'cd ~/shamanov.i/JavaSparkAirlineDelay/logs && cat " + runId + ".log'"))
            {
                cmd.Execute();
                resBuilder.AppendLine("Command>" + cmd.CommandText);
                resBuilder.AppendFormat("Return Value = {0}\n", cmd.ExitStatus);

                string[] separator = { "\n\n" };
                var resParts = cmd.Result.Split(separator, StringSplitOptions.RemoveEmptyEntries);

                foreach (var part in resParts)
                {
                    if (part.Contains("LogType:stdout"))
                    {
                        Regex pattern = new Regex(@"LogLength:(?<logLength>\d+)");
                        Match match = pattern.Match(part);
                        if (match.Groups["logLength"].Value != "0")
                            resBuilder.AppendLine(part);
                    }
                }
            }
        }
    }
}