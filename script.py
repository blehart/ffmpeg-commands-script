#!/usr/bin/python

"""Perform ffmpeg operations on directories of audio files."""

import glob
import json
import click
import subprocess
from os import makedirs, remove, rmdir
from os.path import basename, join
from functools import partial
from itertools import count
from multiprocessing import cpu_count
from multiprocessing.dummy import Pool

@click.group()
@click.argument('path', type=click.Path(exists=True))
@click.pass_context
def cli(ctx, path):
    if path.endswith('/'):
        path = path[:-1]
    ctx.obj = (path, directory_setup(path))


def directory_setup(path):
    """Create directory to put the new files in."""
    for i in count(1):
        new_path = path + str(i)
        try:
            makedirs(new_path)
            break
        except:
            pass
    return new_path


@cli.command(name='speed')
@click.option('-s', '--speed', default=2.0, show_default=True, type=click.FloatRange(1.1, 4.0))
@click.pass_obj
def convert_speed(paths, speed):
    """Increase the speed of files by a factor of $speed."""
    _run_commands([f'ffmpeg -i "{input_file}" -filter:a "atempo={speed / 2},atempo=2.0" -c:a libmp3lame -q:a 4 "{join(paths[1], basename(input_file))}"' for input_file in glob.glob(join(paths[0], '*.mp3'))])


@cli.command(name='type')
@click.pass_obj
def convert_type(paths):
    """Converts m4a files into mp3 files"""
    _run_commands([f'ffmpeg -i "{input_file}" -b:a 192k -vn "{join(paths[1], basename(input_file).replace(".m4a", ".mp3"))}"' for input_file in glob.glob(join(paths[0], '*.m4a'))])


@cli.command(name='duration')
@click.option('-d', '--duration', default=60, show_default=True, type=int, help='Specify the duration in minutes for the change duration operation')
@click.pass_obj
def convert_duration(paths, duration):
    """Concat and then split into files of duration $duration."""
    duration = duration * 60
    temp_file = join(paths[1], 'temp.mp3')
    _concat_files([(sorted(glob.glob(join(paths[0], '*.mp3'))), temp_file)])

    total_duration = float(subprocess.run(f'ffprobe -i "{temp_file}" -show_entries format=duration -v quiet -of csv="p=0"', stdout=subprocess.PIPE, shell=True).stdout)
    num_files = round(total_duration / duration)

    start_duration_filename = []
    start = 0
    for i in range(num_files):
        if i == num_files - 1:
            duration += total_duration % duration
        output_file = join(paths[1], f'output{i}.mp3')
        start_duration_filename.append((start, duration, output_file))
        start += duration

    _split_file(temp_file, start_duration_filename)
    remove(temp_file)


@cli.command(name='chapters')
@click.pass_obj
def split_chapters(paths):
    """Split a file into the chapters defined in the metadata."""
    filename = glob.glob(paths[0] + '/*')[0]
    completed_process = subprocess.run(f'ffprobe -i "{filename}" -print_format json -show_chapters -loglevel error', stdout=subprocess.PIPE, shell=True)
    chapter_info = json.loads(completed_process.stdout)['chapters']
    if not chapter_info:
        print('Chapters Not Found')
        rmdir(paths[1])
    else:
        _split_file(filename, [(float(entry['start_time']), float(entry['end_time']) - float(entry['start_time']), join(paths[1], entry['tags']['title'] + '.mp3')) for entry in chapter_info])


@cli.command(name='concat')
@click.option('-bs', '--batchsize', default=1000, type=int, help='Specify how many files you want to concat together at a time')
@click.pass_obj
def concat_files(paths, batch_size):
    """Reduce the number of files by concating them together in groups of $batchsize."""
    files = sorted(glob.glob(join(paths[0], '*.mp3')))
    batch_size = min(batch_size, len(files))
    input_lists = [files[i:i+batch_size] for i in range(0, len(files), batch_size)]
    _concat_files([(input_list, join(paths[1], f'output{i}.mp3')) for i, input_list in enumerate(input_lists)])


def _split_file(path, split_info):
    "Given an input file, and a list of tuples containing start time, duration, and an output file.  Split the input file."""
    _run_commands([f'ffmpeg -i "{path}" -acodec copy -t {duration} -ss {start} "{output_file}"' for start, duration, output_file in split_info])


def _concat_files(input_output_list):
    """Given a list of tuples, each tuple containing a list of input files, and one output file.  Concat input files into output files."""
    _run_commands([f'ffmpeg -i "concat:{"|".join(input_list)}" -acodec copy "{output_file}"' for input_list, output_file in input_output_list])


def _run_commands(commands):
    """Run a set of command line operations in parallel, cpu_count - 1 at a time."""
    pool = Pool(cpu_count() - 1)
    pool.map(partial(subprocess.run, shell=True), commands)


if __name__ == "__main__":
    cli()
