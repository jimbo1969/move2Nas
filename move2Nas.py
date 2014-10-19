#!/usr/bin/python
"""
Usage: move2Nas.py [-h] [-destination_folder [DESTINATION_FOLDER]]
                   [-logfile [LOGFILE]]
                   [-loglevel [{DEBUG,INFO,WARNING,ERROR,CRITICAL}]]
                   source_folder user password host
The following arguments are required: source_folder, user, password, host
example:
move2Nas.py "C:\Documents and Settings\James Wing\My Documents\My Pictures\Sheds" myuser mypassword DISKSTATION -df
  "photo" -proc_list iexplore.exe pythonw.exe -logfile "C:\Documents and Settings\James Wing\My Documents\move2NAS.log"
"""
import pysftp as sftp
import psutil
import logging
import os
import argparse
import ntpath
import sys

# global variables/constants/handles
s = None


def upload(file):
    global s
    try:
        s.put(file, confirm=True)  # upload the file & confirm size
    except IOError as e:  # IOError is raised if the filesize does not match upon completion of put() with confirm=True
        emsg = "I/O error uploading '{}' ({}): {}".format(file, e.errno, e.strerror)
        print(emsg)
        logging.error(emsg)
        return False  # upload failed
    except Exception as e:  # catch exception but don't bubble up -- keep trying
        emsg = 'unexpected error uploading file: {} ({})'.format(file, str(e))
        print(emsg)
        logging.exception(emsg)
        return False
    return True  # success!


def main(host, user, password, source_folder, destination_folder='', **kwargs):
    global s
    writers = kwargs.get('proc_list')  # list of all processes that may have files open

    log = True if kwargs.get('loglevel') == 'DEBUG' else False  # to log the sftp connection itself {True or False}

    try:
        os.chdir(source_folder)  # set local working folder
    except Exception as e:
        emsg = 'cannot access source_folder: {}\n{}'.format(source_folder, e)
        print(emsg)
        logging.exception(emsg)
        raise

    upload_filelist = os.listdir(".")  # add all files in working folder to upload list
    msg = 'Files found in folder ' + source_folder + ':\n' + '\n'.join(upload_filelist)
    logging.info(msg)

    msg = 'We are excluding any files with handles held by these processes:\n' + '\n'.join(writers)
    logging.info(msg)
    do_not_upload_filelist = []  # list of all files that we must not upload yet (due to being written by other process)
    try:
        # get all processes that may be writing files (defined above)
        procs = filter(lambda p: any(", name='" + proc_name + "')" in str(p.name)
                                     for proc_name in writers), psutil.process_iter())
        for proc in procs:
            try:
                # get all open files belonging to the process that are in the folder of interest (source_folder)
                # 1) get openfiles tuples of interest
                popenfiles = filter(lambda pof: ntpath.dirname(pof.path) != source_folder, proc.open_files())
                # 2) get a list of just the filenames, sans fullpath
                openfiles = [ntpath.basename(pof.path) for pof in popenfiles]
                # 3) append this list of files open by this process to list of files to not move
                do_not_upload_filelist.extend(openfiles)
            except psutil.NoSuchProcess as e:  # catch when process closes before we get open files
                emsg = ' '.join(["Process disappeared.  'Tis OK.", "\n" + str(e)])
                print(emsg)
                logging.info(emsg)
            except psutil.AccessDenied as e:  # catch access denied to process
                emsg = ' '.join(["Cannot access process to retrieve open file list.",
                      "\nIs this being executed as the proper user?", "\n" + str(e)])
                print(emsg)
                logging.warning(emsg)
    except psutil.AccessDenied as e:  # catch access denied to process
        emsg = ' '.join(["Access Denied to one of the processes specified in proc_list.",
              "\nIs this being executed as the proper user?\nAre the processes running under the expected user?",
              "\nNote that psutil doesn't do Windows well when it comes to open_files().",
              "\nWe won't be able to determine which files are open to avoid trying to move them."])
        print(emsg, str(e))
        logging.warning(emsg, str(e))

    msg = 'The following files in ' + source_folder + ' are open, and will therefore not be uploaded:\n' + \
          '\n'.join(do_not_upload_filelist)
    logging.info(msg)

    # remove all items in the do_not_upload_filelist from the upload_filelist
    upload_filelist = [f for f in upload_filelist if f not in do_not_upload_filelist]
    msg = 'final list of files to upload:\n' + '\n'.join(upload_filelist)
    logging.info(msg)

    if len(upload_filelist) < 1:  # end program if there are no files to move
        msg = "List of files to move is empty, so exiting."
        print('\n' + msg)
        logging.info(msg)
        exit()

    #open the sftp connection and upload files
    logging.info("attempting sftp connection to {} folder on host {} as user {} logging sftp"
                 .format(destination_folder, host, user))
    try:
        s = sftp.Connection(host=host, username=user, password=password, log=log)
    except Exception as e:
        msg = 'Failed to open sftp connection: {}'.format(str(e))
        print(msg)
        logging.exception(msg)
        raise  # bubble up the exception
    else:
        try:
            s.cwd(destination_folder)
        except Exception as e:
            emsg = 'Failed to open destination folder: ' + str(e)
            print(emsg)
            logging.exception(emsg)
            raise  # bubble up the exception
        for target_file in upload_filelist:
            if os.path.isfile(target_file):
                msg = 'moving file: {}'.format(target_file)
                print(msg)
                logging.info(msg)
                if upload(target_file):
                    logging.info('deleting local file: {}'.format(target_file))
                    os.remove(target_file)  # delete local copy of file
    finally:
        logging.info('closing sftp connection')
        s.close()


if __name__ == "__main__":

    # create dictionary to lookup loglevels (imported from logging module)
    loglevels = dict({
        'CRITICAL': logging.CRITICAL,
        'ERROR': logging.ERROR,
        'WARNING': logging.WARNING,
        'INFO': logging.INFO,
        'DEBUG': logging.DEBUG,
        'NOTSET': logging.NOTSET
    })
    loglevels.setdefault(logging.WARNING)  # default loglevel to WARNING, as described below in argparse stuff

    parser = argparse.ArgumentParser(description='use sftp to move all files from source folder to '
                                                 'destination folder on remote sftp server')
    parser.add_argument('source_folder', help='source folder of local files '
                                              '(warning: "./" is a bad idea, as it will move this program too)')
    parser.add_argument('user', help='username on remote sftp server (e.g. pi)')
    parser.add_argument('password', help='password on remote sftp server (e.g. raspberry)')
    parser.add_argument('host', help='sftp server hostname or IP address (e.g. Diskstation or 192.168.0.55')
    parser.add_argument('-destination_folder', '-df', nargs='?', default='',
                        help='destination folder on remote sftp server (default: home_folder)')
    parser.add_argument('-proc_list', '-pl', nargs='*', help='skip files open by these processes')
    parser.add_argument('-logfile', '-lf', nargs='?', default='move2NAS.log',
                        help='name of file to log info and errors (default: move2NAS.log)')
    parser.add_argument('-loglevel', '-ll', nargs='?', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
                        default='WARNING', help='loglevel for the process: {DEBUG} {INFO} {WARNING} {ERROR} {CRITICAL} '
                                                '(default: WARNING)  (only used if logfile is specified)')
    args = parser.parse_args()

    # set up logging
    logfile = args.logfile
    loglevel = loglevels[args.loglevel]
    logging.basicConfig(filename=logfile, filemode='w', level=loglevel,
                        format='%(asctime)s - %(levelname)s - %(message)s',
                       )
    # log all arguments except password for troubleshooting purposes
    msg = ['Executing script: ' + sys.argv[0] + '.\nCommandline arguments are listed below:']
    for key, value in vars(args).items():
        msg.append('{} = {}'.format(key, value if key != 'password' else '******'))
    logging.info('\n'.join(msg))

    main(**vars(args))  # execute main program, passing indeterminate-length list of arguments
    logging.shutdown()  # flush & close logger