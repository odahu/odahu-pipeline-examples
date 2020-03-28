import sys
import os


def print_version(ds, *args, **kwargs):
    print(f'ds: {ds}')
    print(f'args: {args}')
    print(f'kwargs: {kwargs}')
    print(f'version: {sys.version}')
    print(f'cwd: {os.getcwd()}')
    print(f'uname: {os.uname()}')
    os.mkdir('test1234')


def print_param(ds, param, *args, **kwargs):
    print(f'param: {param}')