#!/usr/bin/python

"""Perform ffmpeg operations on directories of audio files."""

import glob
import json
import click
import subprocess
from os import makedirs, remove
from os.path import basename, join
from functools import partial
from multiprocessing import cpu_count
from multiprocessing.dummy import Pool

FFMPEG_TEMPLATE = 'ffmpeg -i "{0}" {1} "{2}"'
FFPROBE_TEMPLATE = 'ffprobe -i "{0}" {1}'

@click.group()
@click.argument('path', type=click.Path(exists=True))
@click.pass_context
def cli(ctx, path):
    ctx.obj = (path, directory_setup(path))


def directory_setup(path):
    """Create directory to put the new files in."""
    i = 1
    while True:
        new_path = path + str(i)
        try:
            makedirs(new_path)
            break
        except:
            i += 1
    return new_path


@cli.command(name='speed')
@click.option('-s', '--speed', default=2.0, show_default=True, type=click.FloatRange(1.1, 4.0))
@click.pass_obj
def convert_speed(paths, speed):
    if speed > 2.0:
        speed_b = speed / 2
        speed = 2.0
    else:
        speed_b = 1.0

    input_file_type = '*.mp3'
    output_file_func = lambda input_f: join(paths[1], basename(input_f))
    parameters = '-filter:a "atempo={0},atempo={1}" -c:a libmp3lame -q:a 4'.format(speed, speed_b)

    convert_(paths[0], input_file_type, output_file_func, parameters)


@cli.command(name='type')
@click.pass_obj
def convert_type(paths):
    input_file_type = '*.m4a'
    output_file_func = lambda input_f: join(paths[1], basename(input_f).replace('.m4a', '.mp3'))
    parameters = '-b:a 192k -vn'

    convert_(paths[0], input_file_type, output_file_func, parameters)


def convert_(old_path, input_file_type, output_file_func, parameters):
    commands = []
    for input_file in glob.glob(join(old_path, input_file_type)):
        output_file = output_file_func(input_file)
        commands.append(FFMPEG_TEMPLATE.format(input_file, parameters, output_file))

    _run_commands(commands)


@cli.command(name='duration')
@click.option('-d', '--duration', default=60, show_default=True, type=int, help='Specify the duration in minutes for the change duration operation')
@click.pass_obj
def convert_duration(paths, duration):
    """Concat mp3 files and then split into files of duration $duration."""
    duration = duration * 60
    files = sorted(glob.glob(join(paths[0], '*.mp3')))
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
        output_file = join(paths[1], 'output{0}.mp3'.format(i))
        start_duration_filename.append((start, duration, output_file))
        start += duration

    _split_file(temp_file, start_duration_filename)
    remove(temp_file)


@cli.command(name='chapters')
@click.pass_obj
def split_chapters(paths):
    filename = glob.glob(paths[0] + '/*')[0]
    parameters = '-print_format json -show_chapters -loglevel error'
    completed_process = subprocess.run(FFPROBE_TEMPLATE.format(filename, parameters), stdout=subprocess.PIPE, shell=True)
    chapters_info = json.loads(completed_process.stdout)['chapters']
    start_duration_filename = []
    for entry in chapters_info:
        start_time = float(entry['start_time'])
        duration = float(entry['end_time']) - start_time
        output_file = join(paths[1], entry['tags']['title'] + '.mp3')
        start_duration_filename.append((start_time, duration, output_file))
    
    _split_file(filename, start_duration_filename)


@cli.command(name='concat')
@click.option('-bs', '--batchsize', default=0, type=int, help='Specify how many files you want to concat together at a time')
@click.pass_obj
def concat_files(paths, batch_size):
    files = sorted(glob.glob(join(paths[0], '*.mp3')))

    if batch_size == 0:
        batch_size = len(files)

    input_lists = [files[i:i+batch_size] for i in range(0, len(files), batch_size)]

    input_output_list = []
    for i, input_list in enumerate(input_lists):
        output_file = join(paths[1], 'output{0}.mp3'.format(i))
        input_output_list.append((input_list, output_file))

    _concat_files(input_output_list)


def _split_file(path, start_duration_filename):
    "Given an input file, and a list of tuples containing start time, duration, and an output file.  Split the input file."""
    commands = []
    for start, duration, output_file in start_duration_filename:
        parameters = '-acodec copy -t {0} -ss {1}'.format(duration, start)
        commands.append(FFMPEG_TEMPLATE.format(path, parameters, output_file))
    _run_commands(commands)


def _concat_files(input_output_list):
    """Given a list of tuples, each tuple containing a list of input files, and one output file.  Concat input files into output files."""
    parameters = '-acodec copy'
    commands = []
    for input_list, output_file in input_output_list:
        input_files = 'concat:' + '|'.join(input_list)
        commands.append(FFMPEG_TEMPLATE.format(input_files, parameters, output_file))
    _run_commands(commands)


def _run_commands(commands):
    """Run a set of command line operations in parallel, cpu_count - 1 at a time."""
    pool = Pool(cpu_count() - 1)
    pool.map(partial(subprocess.run, shell=True), commands)


if __name__ == "__main__":
    cli()
