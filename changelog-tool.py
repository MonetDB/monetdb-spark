#!/usr/bin/env python3

import argparse
import datetime
import os
import re
import shutil
import sys
import tempfile

CHANGELOG = 'CHANGELOG.md'

UNRELEASED_TEMPLATE = """
# Unreleased


"""


def valid_version_number(s):
    m = re.match('^v?([0-9]+([.][0-9]+)+)$', s)
    if not m:
        raise ValueError(f'version number {s!r} does not match vX.Y.Z')
    return m.group(1)


def full_path():
    return os.path.join(os.path.dirname(__file__), CHANGELOG)

def read_sections():
    """Return a dictionary str->list(str) mapping versions to the lines of the
    corresponding section of the CHANGELOG
    """

    # yes, this also matches 'vUnreleased', don't care
    heading_pattern = re.compile('^# v?(Unreleased|([0-9]+([.][0-9]+)+))(\s+-.*)?$')

    cur_list = []
    sections = {}
    with open(full_path()) as f:
        for i0, line in enumerate(f):
            i = i0 + 1
            line = line.rstrip()
            if line.startswith('# '):
                m = heading_pattern.match(line)
                if m is None:
                    raise Exception(f'line {i}: invalide header: {line}')
                version = m.group(1)
                cur_list = sections[version] = []
            cur_list.append(line)

    for l in sections.values():
        while l and l[-1] == '':
            del l[-1]

    return sections


def get_section(version):
    sections = read_sections()
    contents = sections.get(version)
    if contents:
        return contents
    print(f'Version {version} is not in {CHANGELOG}', file=sys.stderr)
    exit(1)


def do_title(version):
    contents = get_section(version)
    title = contents[0][1:].strip()
    print(title)


def do_body(version):
    contents = get_section(version)
    body = '\n'.join(contents[1:]).strip()
    print(body)


def do_rename(version):
    datestamp = str(datetime.date.today())
    sections = read_sections()
    if version in sections:
        print(f'Version {version} already exists in {CHANGELOG}', file=sys.stderr)
        exit(1)

    dest = full_path()
    with tempfile.NamedTemporaryFile('w', prefix=dest) as f:
        print(UNRELEASED_TEMPLATE.strip(), file=f)
        for section, content in sections.items():
            if section == 'Unreleased':
                content[0] = f'# v{version} - {datestamp}'
            print(file=f)
            print(file=f)
            for line in content:
                print(line, file=f)

        # commit
        f.flush()
        shutil.copyfile(f.name, dest)



if __name__ == "__main__":
    argparser = argparse.ArgumentParser()
    group = argparser.add_mutually_exclusive_group(required=True)
    group.add_argument('--check', action='store', type=valid_version_number, help='Check if a section exists for this version number')
    group.add_argument('--rename', action='store', type=valid_version_number, help='Rename Unreleased to this version number')
    group.add_argument('--title', action='store', type=valid_version_number, help='Give the full title of the section for this version number')
    group.add_argument('--body', action='store', type=valid_version_number, help='Give body of the section for this version number')

    args = argparser.parse_args()
    #print(args, file=sys.stderr)

    if args.check is not None:
        get_section(args.check)
    elif args.rename is not None:
        do_rename(args.rename)
    elif args.title is not None:
        do_title(args.title)
    elif args.body is not None:
        do_body(args.body)




