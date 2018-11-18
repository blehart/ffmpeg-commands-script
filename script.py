#!/usr/bin/python

"""Perform ffmpeg operations on directories of audio files."""

import glob
import json
import argparse
import subprocess
from os import makedirs, remove
from os.path import basename, join
from functools import partial
from multiprocessing import cpu_count
from multiprocessing.dummy import Pool

class FloatRange():
    """Tells the Argument Parser whether the given float is within a certain range."""

    def __init__(self, start, end):
        self.start = start
        self.end = end

    def __contains__(self, other):
        return self.start <= other <= self.end

    def __getitem__(self, index):
        if index == 0:
            return self
        raise IndexError()

    def __repr__(self):
        return '{0}-{1}'.format(self.start, self.end)

parser = argparse.ArgumentParser()
parser.add_argument('path', help='Directory to perform the operation on')
parser.add_argument('op', choices=['type', 'speed', 'duration', 'chapters', 'concat'], help='Operation to perform')
parser.add_argument('-s', '--speed', default=2.0, type=float, choices=FloatRange(1.1, 4.0),
                    help='Specify the change in speed for the change speed operation')
parser.add_argument('-d', '--duration', default=60, type=int,
                    help='Specify the duration in minutes for the change duration operation')
parser.add_argument('-bs', '--batchsize', default=0, type=int,
                    help='Specify how many files you want to concat together at a time')

FFMPEG_TEMPLATE = 'ffmpeg -i "{0}" {1} "{2}"'
FFPROBE_TEMPLATE = 'ffprobe -i "{0}" {1}'

def handle_operation(args):
    """Decide which operation to call based on CL arguments."""
    new_path = directory_setup(args.path)
    if args.op == 'type' or args.op == 'speed':
        convert_(args.op, args.path, new_path, args.speed)
    elif args.op == 'duration':
        convert_duration(args.path, new_path, args.duration * 60)
    elif args.op == 'chapters':
        split_chapters(args.path, new_path)
    elif args.op == 'concat':
        concat_files(args.path, new_path, args.batchsize)


def directory_setup(path):
    """Create directory to put the new files in."""
    new_path = 'new' + path
    i = 2
    while True:
        print(new_path)
        try:
            makedirs(new_path)
            break
        except:
            new_path = 'new' + str(i) + path
            i += 1
    return new_path


def convert_(op, old_path, new_path, speed_a):
    """Perform the given op on files.

    Speed - Multiple the speed of mp3 files by speed_a
    Type - Convert m4a files to mp3 files
    """
    if op == 'speed':
        if speed_a > 2.0:
            speed_b = speed_a / 2
            speed_a = 2.0
        else:
            speed_b = 1.0

        input_file_type = '*.mp3'
        output_file_func = lambda input_f: join(new_path, basename(input_f))
        parameters = '-filter:a "atempo={0},atempo={1}" -c:a libmp3lame -q:a 4'.format(speed_a, speed_b)
    elif op == 'type':
        input_file_type = '*.m4a'
        output_file_func = lambda input_f: join(new_path, basename(input_f).replace('.m4a', '.mp3'))
        parameters = '-b:a 192k -vn'

    commands = []
    for input_file in glob.glob(join(old_path, input_file_type)):
        output_file = output_file_func(input_file)
        commands.append(FFMPEG_TEMPLATE.format(input_file, parameters, output_file))

    run_commands(commands)


def convert_duration(old_path, new_path, duration):
    """Concat mp3 files and then split into files of duration $duration."""
    files = sorted(glob.glob(join(old_path, '*.mp3')))
    temp_file = join(new_path, 'temp.mp3')
    _concat_files([(files, temp_file)])
    
    parameters = '-show_entries format=duration -v quiet -of csv="p=0"'
    completed_process = subprocess.run(FFPROBE_TEMPLATE.format(temp_file, parameters), stdout=subprocess.PIPE, shell=True)
    total_duration = float(completed_process.stdout)
    num_files = round(total_duration / duration)

    start_duration_filename = []
    start = 0
    for i in range(num_files):
        if i == num_files - 1:
            duration += total_duration % duration
        output_file = join(new_path, 'output{0}.mp3'.format(i))
        start_duration_filename.append((start, duration, output_file))
        start += duration

    _split_file(temp_file, start_duration_filename)
    remove(temp_file)


def split_chapters(old_path, new_path):
    filename = glob.glob(old_path + '/*')[0]
    parameters = '-print_format json -show_chapters -loglevel error'
    completed_process = subprocess.run(FFPROBE_TEMPLATE.format(filename, parameters), stdout=subprocess.PIPE, shell=True)
    chapters_info = json.loads(completed_process.stdout)['chapters']
    start_duration_filename = []
    for entry in chapters_info:
        start_time = float(entry['start_time'])
        duration = float(entry['end_time']) - start_time
        output_file = join(new_path, entry['tags']['title'] + '.mp3')
        start_duration_filename.append((start_time, duration, output_file))
    
    _split_file(filename, start_duration_filename)


def concat_files(old_path, new_path, batch_size):
    files = sorted(glob.glob(join(old_path, '*.mp3')))

    if batch_size == 0:
        batch_size = len(files)

    input_lists = [files[i:i+batch_size] for i in range(0, len(files), batch_size)]

    input_output_list = []
    for i, input_list in enumerate(input_lists):
        output_file = join(new_path, 'output{0}.mp3'.format(i))
        input_output_list.append((input_list, output_file))

    _concat_files(input_output_list)


def _split_file(path, start_duration_filename):
    "Given an input file, and a list of tuples containing start time, duration, and an output file.  Split the input file."""
    commands = []
    for start, duration, output_file in start_duration_filename:
        parameters = '-acodec copy -t {0} -ss {1}'.format(duration, start)
        commands.append(FFMPEG_TEMPLATE.format(path, parameters, output_file))
    run_commands(commands)


def _concat_files(input_output_list):
    """Given a list of tuples, each tuple containing a list of input files, and one output file.  Concat input files into output files."""
    parameters = '-acodec copy'
    commands = []
    for input_list, output_file in input_output_list:
        input_files = 'concat:' + '|'.join(input_list)
        commands.append(FFMPEG_TEMPLATE.format(input_files, parameters, output_file))
    run_commands(commands)


def run_commands(commands):
    """Run a set of command line operations in parallel, cpu_count - 1 at a time."""
    pool = Pool(cpu_count() - 1)
    pool.map(partial(subprocess.run, shell=True), commands)


if __name__ == "__main__":
    handle_operation(parser.parse_args())
