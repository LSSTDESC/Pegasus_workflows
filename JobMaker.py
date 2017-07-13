import os
import Pegasus.DAX3 as DAX3

class JobMaker(object):
    def __init__(self, dax, repo, config_dir):
        self.dax = dax
        self.repo = repo
        self.config_dir = config_dir

    def make(self, task_name, dataId=None, options=None, repo=None):
        job = DAX3.Job('%s.py' % task_name)
        if repo is None:
            repo = self.repo
        args = [repo]
        args = self._add_dataId(args, dataId)
        args = self._add_options(args, options)
        configfile = os.path.join(self.config_dir, '%s-config.py' % task_name))
        args.extend(['--configfile', configfile])
        job.addArguments(*args)
        self.dax.addJob(job)
        return job

    def _add_dataId(self, args, dataId):
        if dataId is None:
            return args
        args.append('--id')
        for key, value in dataId.items():
            args.append('%s=%s' % (key, value))
        return args

    def _add_options(self, args, options):
        if options is None:
            return args
        for key, value in options.items():
            args.append([key, value])
        return args
