using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Web;
using System.Web.Mvc;

namespace TestMvcSite.Controllers
{
    public class HomeController : Controller
    {
        public static bool printResult = false;
        public static string execResult = "";

        public ActionResult Index()
        {
            ViewBag.ShowResult = printResult;

            if (printResult)
            {
                ViewBag.ResultTitle = "Task logs:";
                ViewBag.ResultBody = execResult.ToString();

                printResult = false;
                execResult = "";
            }
            
            return View();
        }

        [HttpPost]
        public ActionResult Upload()
        {
            if (Request.Files.Count > 0)
            {
                var file = Request.Files[0];

                if (file != null && file.ContentLength > 0)
                {
                    var fileName = Path.GetFileName(file.FileName);
                    var path = Path.Combine(Server.MapPath("~/"), fileName);
                    file.SaveAs(path);
                    
                    int number = -1;
                    try { number = Int32.Parse(Request.Form["number"]); } catch (Exception) { }

                    if (number > 0)
                    {
                        execResult = SshWrapper.ProcessTask(path, number);
                        printResult = true;
                    }
                    else
                    {
                        execResult = "Set number of executors first!";
                        printResult = true;
                    }
                }
                else
                {
                    execResult = "Choose file first!";
                    printResult = true;
                }   
            }
            
            return RedirectToAction("Index");
        }
    }
}