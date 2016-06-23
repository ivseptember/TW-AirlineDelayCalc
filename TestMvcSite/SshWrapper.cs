using System;
using Renci.SshNet;
using System.IO;
using System.Text.RegularExpressions;
using System.Text;

namespace TestMvcSite
{
    public class SshWrapper
    {
        public static string ProcessFile(string uploadfile, int executorcount)
        {
            StringBuilder resBuilder = new StringBuilder();

            const int port = 57322;
            const string host = "79.170.167.30";
            const string username = "user3";
            const string password = "userIntelPhi3";
            const string workingdirectory = "/home/user3/shamanov.i/JavaSparkAirlineDelay/";

            // Upload data file
            resBuilder.AppendLine("Creating client and connecting");
            using (var sftpclient = new SftpClient(host, port, username, password))
            {
                sftpclient.Connect();
                resBuilder.AppendFormat("Connected to {0}", host);

                sftpclient.ChangeDirectory(workingdirectory);
                resBuilder.AppendFormat("Changed directory to {0}", workingdirectory);

                using (var fileStream = new FileStream(uploadfile, FileMode.Open))
                {
                    resBuilder.AppendFormat("Uploading {0} ({1:N0} bytes)", uploadfile, fileStream.Length);
                    sftpclient.BufferSize = 4 * 1024; // bypass Payload error large files
                    sftpclient.UploadFile(fileStream, Path.GetFileName(uploadfile));
                }
            }

            // Execute a (SHELL) Commands 
            using (var sshclient = new SshClient(host, port, username, password))
            {
                int retValue = 0;
                string runId = "";
                string result = "";

                sshclient.Connect();

                using (var cmd = sshclient.CreateCommand("ssh node23 'cd ~/shamanov.i/JavaSparkAirlineDelay/ && hadoop fs -copyFromLocal -f " + Path.GetFileName(uploadfile) + " /user/user3/shamanov.i/JavaSparkAirlineDelay'"))
                {
                    cmd.Execute();
                    resBuilder.AppendLine("Command>" + cmd.CommandText);
                    resBuilder.AppendFormat("Return Value = {0}", cmd.ExitStatus);

                    retValue = cmd.ExitStatus;
                }

                if (retValue == 0)
                {
                    using (var cmd = sshclient.CreateCommand("ssh node23 'cd ~/shamanov.i/JavaSparkAirlineDelay/ && spark-submit --class JavaSparkAirlineDelay --master yarn --deploy-mode cluster --executor-memory 1024m --num-executors 2 JavaSparkAirlineDelay.jar shamanov.i/JavaSparkAirlineDelay/" + Path.GetFileName(uploadfile) + "'"))
                    {
                        cmd.Execute();
                        resBuilder.AppendLine("Command>" + cmd.CommandText);
                        resBuilder.AppendFormat("Return Value = {0}", cmd.ExitStatus);

                        var reader = new StreamReader(cmd.ExtendedOutputStream);
                        result = reader.ReadToEnd();

                        retValue = cmd.ExitStatus;
                    }
                }

                if (retValue == 0)
                {
                    Regex pattern = new Regex(@"tracking URL:.*proxy\/(?<runId>[^\/]+)\/");
                    Match match = pattern.Match(result);
                    runId = match.Groups["runId"].Value;

                    using (var cmd = sshclient.CreateCommand("ssh node23 'cd ~/shamanov.i/JavaSparkAirlineDelay/logs && yarn logs -applicationId " + runId + " > " + runId + ".log'"))
                    {
                        cmd.Execute();
                        resBuilder.AppendLine("Command>" + cmd.CommandText);
                        resBuilder.AppendFormat("Return Value = {0}", cmd.ExitStatus);

                        retValue = cmd.ExitStatus;
                    }
                }

                if (retValue == 0)
                {
                    using (var cmd = sshclient.CreateCommand("ssh node23 'cd ~/shamanov.i/JavaSparkAirlineDelay/logs && cat " + runId + ".log'"))
                    {
                        cmd.Execute();
                        resBuilder.AppendLine("Command>" + cmd.CommandText);
                        resBuilder.AppendFormat("Return Value = {0}", cmd.ExitStatus);

                        resBuilder.AppendLine(cmd.Result);
                    }
                }

                sshclient.Disconnect();
            }

            return resBuilder.ToString();
        }
    }
}