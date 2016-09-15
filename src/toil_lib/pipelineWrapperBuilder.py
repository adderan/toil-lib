from __future__ import print_function

import argparse
import json
import logging
import os
import shutil
import subprocess
import sys
import textwrap
from toil_lib import require, UserError

log = logging.getLogger(__name__)

class PipelineWrapperBuilder(object):
    """
    This class can be used to define wrapper scripts to run specific Toil pipelines in docker
    containers. The purpose of this class is to provide a convenient way to define a command line
    interface for a wrapper script and the logic to run the pipeline with this interface.
    """
    def __init__(self, name, desc, config, add_no_clean=True, add_resume=True):
        """
        :param str name: The name of the command to start the workflow.
        :param str desc: The description of the workflow.
        :param str config: A format string where each key exactly matches the name of an argument
            defined in the arg_builder context manager. Note that dashes in argument names should be
            changed to underscores in this string e.g. 'no-clean' should be 'no_clean'.
        """
        self._name = name
        self._desc = desc
        self._config = config
        self._no_clean = add_no_clean
        self._resume = add_resume

    def run(self, args, pipeline_command):
        """
        Invokes the pipeline with the defined command. Command line arguments, and the command need
        to be set with arg_builder, and command_builder respectively before this method can be
        invoked.
        """
        # prepare workdir
        mount = self._prepare_mount()
        self._workdir = os.path.join(mount, 'Toil-' + self._name)
        # prepare config
        args_dict = vars(args)
        args_dict['output_dir'] = mount
        self._config = textwrap.dedent(self._config.format(**args_dict))
        config_path = os.path.join(self._workdir, 'config')
        command = self._make_command_prefix(os.path.join(self._workdir, 'jobStore'),
                                            config_path,
                                            self._workdir) + pipeline_command
        if self._resume and args.resume:
            command.append('--restart')
        self._create_workdir(args)
        with open(config_path, 'w') as f:
            f.write(self._config)

        try:
            subprocess.check_call(command)
        except subprocess.CalledProcessError as e:
            print(e, file=sys.stderr)
        finally:
            log.info('Pipeline terminated, changing ownership of output files from root to user.')
            stat = os.stat(self._mount)
            subprocess.check_call(['chown', '-R', '{}:{}'.format(stat.st_uid, stat.st_gid),
                                   self._mount])
            if self._no_clean and args.no_clean:
                log.info('Flag "--no-clean" was used, therefore %s was not deleted.', self._workdir)
            else:
                log.info('Cleaning up temporary directory: %s', self._workdir)
                shutil.rmtree(self._workdir)

    def get_args(self):
        """
        This method returns an arg parse object that should be used to build the command line
        interface. Note that some arguments are added by default.
        """
        parser = argparse.ArgumentParser(description=self._desc,
                                         formatter_class=argparse.RawTextHelpFormatter)
        # default args
        if  self._no_clean:
            parser.add_argument('--no-clean', action='store_true',
                                help='If this flag is used, temporary work directory is not '
                                     'cleaned.')
        if self._resume:
            parser.add_argument('--resume', action='store_true',
                                help='If this flag is used, a previously uncleaned workflow in the'
                                     ' same directory will be resumed')
        return parser

    def _prepare_mount(self):
        # Get name of most recent running container. If socket is mounted, should be this one.
        name_command = ['docker', 'ps', '--format', '{{.Names}}']
        try:
            name = subprocess.check_output(name_command).split('\n')[0]
        except subprocess.CalledProcessError as e:
            raise RuntimeError('No container detected, ensure Docker is being run with: '
                               '"-v /var/run/docker.sock:/var/run/docker.sock" as an argument.'
                               ' \n\n{}'.format(e.message))
        # Get name of mounted volume
        blob = json.loads(subprocess.check_output(['docker', 'inspect', name]))
        mounts = blob[0]['Mounts']
        # Ensure docker.sock is mounted correctly
        sock_mnt = [x['Source'] == x['Destination'] for x in mounts if 'docker.sock' in x['Source']]
        require(len(sock_mnt) == 1, 'Missing socket mount. Requires the following: '
                                      'docker run -v /var/run/docker.sock:/var/run/docker.sock')
        # Ensure formatting of command for 2 mount points
        if len(mounts) == 2:
            require(all(x['Source'] == x['Destination'] for x in mounts),
                    'Docker Src/Dst mount points, invoked with the -v argument, '
                    'must be the same if only using one mount point aside from the docker socket.')
            work_mount = [x['Source'] for x in mounts if 'docker.sock' not in x['Source']]
        else:
            # Ensure only one mirror mount exists aside from docker.sock
            mirror_mounts = [x['Source'] for x in mounts if x['Source'] == x['Destination']]
            work_mount = [x for x in mirror_mounts if 'docker.sock' not in x]
            require(len(work_mount) == 1, 'Wrong number of mirror mounts provided, see '
                                          'documentation.')
        # Output log information
        log.info('The work mount is: %s', work_mount[0])
        self._mount = work_mount[0]
        return self._mount

    def _make_command_prefix(self, jobstore_path, config_path, workdir_path):
        return [self._name, 'run', jobstore_path,
                '--config', config_path,
                '--workDir', workdir_path,
                '--retryCount', '1']

    def _create_workdir(self, args):
        if os.path.exists(self._workdir):
            if self._resume and args.resume:
                 log.info('Reusing temporary directory: %s', self._workdir)
            else:
                raise UserError('Temporary directory {} already exists. Run with --resume option or'
                                ' remove directory.'.format(self._workdir))
        else:
            os.makedirs(self._workdir)
            log.info('Temporary directory created: %s', self._workdir)
