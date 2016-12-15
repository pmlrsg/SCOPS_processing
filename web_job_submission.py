###########################################################
# This file has been created by ARSF Data Analysis Node and
# is licensed under the GPL v3 Licence. A copy of this
# licence is available to download with this file.
###########################################################
"""
Classes for job submission.
"""
import subprocess
import os
import web_process_apl_line
import web_common

class JobSubmission(object):
   """
   Abstract class for job submission.

   Different methods inherit from this.
   """

   def __init__(self, logger, defaults):
      self.logger = logger
      self.defaults = defaults

   def submit(self, config, line, output_location, filesizes,
              main_line, band_ratio):
      """
      Job submission. Classes must provide an implementation of this
      """
      raise NotImplementedError

   def get_name(self):
      """
      Short name for job submission system
      """
      raise NotImplementedError

class LocalJobSubmission(JobSubmission):
   """
   Job submission class for running locally.
   """
   def submit(self, config, line, output_location, filesizes,
              main_line, band_ratio):

      try:
         self.logger.info("processing line {}".format(line))
         web_process_apl_line.line_handler(config, line, output_location,
                                           main_line, band_ratio)
      except Exception as e:
         self.logger.error("Could not process job for {}, "
                           "Reason: {}".format(line, e))

   def get_name(self):
      return "local"

class QsubJobSubmission(JobSubmission):
   """
   Job submission class for the Sun Grid Engine (SGE)
   using qsub
   """
   def submit(self, config, line, output_location, filesizes,
              main_line, band_ratio):

      qsub_args = ["qsub"]
      qsub_args.extend(["-N", "WEB_" + self.defaults["project_code"] + "_" + line])
      qsub_args.extend(["-q", web_common.QUEUE])
      qsub_args.extend(["-P", web_common.QSUB_PROJECT])
      qsub_args.extend(["-p",0])
      qsub_args.extend(["-wd", web_common.WEB_OUTPUT])
      qsub_args.extend(["-e", web_common.QSUB_LOG_DIR])
      qsub_args.extend(["-o", web_common.QSUB_LOG_DIR])
      qsub_args.extend(["-m", "n"]) # Don't send mail
      qsub_args.extend(["-b", "y"])
      qsub_args.extend(["-l", "apl_throttle=1"])
      qsub_args.extend(["-l", "apl_web_throttle=1"])
      try:
         if not filesizes is None:
            filesize = int([x for x in filesizes if line in x][0].split(",")[1].replace("G\n", ""))
            filesize += filesize * 0.5
         else:
            #we couldnt find a filesize - default to 100GB
            filesize = 100
      except Exception as e:
         #something went wrong so we should default to 100GB
         filesize = 100
      qsub_args.extend(["-l", "tmpfree={}".format(filesize)])
      script_args = [web_common.PROCESS_COMMAND]
      script_args.extend(["-l", line])
      script_args.extend(["-c", config])
      script_args.extend(["-s","fenix"])
      script_args.extend(["-o", output_location])
      if main_line:
         script_args.extend(["-m"])
      if band_ratio:
         script_args.extend(["-b"])

      qsub_args.extend(script_args)
      try:
         self.logger.info("submitting line {}".format(line))
         self.logger.info("qsub command: {}".format(" ".join(qsub_args)))
         qsub = subprocess.Popen(qsub_args, stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE)
         out, err = qsub.communicate()
         self.logger.info(out)
         if err:
            self.logger.error(err)

         if main_line or band_ratio:
            self.logger.info("line submitted: " + line)
      except Exception as e:
         self.logger.error("Could not submit qsub job. Reason: {}".format(e))

   def get_name(self):
      return "qsub"

class BsubJobSubmission(JobSubmission):
   """
   Job submission class for LSF using bsub
   """
   def submit(self, config, line, output_location, filesizes,
              main_line, band_ratio):

      job_name = "WEB_" + self.defaults["project_code"] + "_" + line
      qsub_args = ["bsub"]
      qsub_args.extend(["-N", job_name])
      qsub_args.extend(["-q", web_common.QUEUE])
      qsub_args.extend(["-e", os.path.join(web_common.QSUB_LOG_DIR, job_name)])
      qsub_args.extend(["-o", os.path.join(web_common.QSUB_LOG_DIR, job_name)])
      qsub_args.extend(["-W", web_common.QSUB_LOG_DIR])
      script_args = [web_common.PROCESS_COMMAND]
      script_args.extend(["-l", line])
      script_args.extend(["-c", config])
      script_args.extend(["-s","fenix"])
      script_args.extend(["-o", output_location])
      if main_line:
         script_args.extend(["-m"])
      if band_ratio:
         script_args.extend(["-b"])

      qsub_args.extend(script_args)
      try:
         self.logger.info("submitting line {}".format(line))
         self.logger.info("qsub command: {}".format(" ".join(qsub_args)))
         qsub = subprocess.Popen(qsub_args, stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE)
         out, err = qsub.communicate()
         self.logger.info(out)
         if err:
            self.logger.error(err)

         if main_line or band_ratio:
            self.logger.info("line submitted: " + line)
      except Exception as e:
         self.logger.error("Could not submit bsub job. Reason: {}".format(e))

   def get_name(self):
      return "bsub"

