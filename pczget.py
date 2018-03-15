"""
Copyright 2018 Ryan Sommers

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""
import concurrent.futures as cf
import json
import os
import re
import requests
import sys
from argparse import ArgumentParser
from functools import partial
from requests_futures.sessions import FuturesSession
from urllib.parse import urlparse, urljoin


def get_output_filename(base_dir: str, url: str):
    purl = urlparse(url)
    return '{0}/{1}.txt.gz'.format(base_dir, os.path.basename(purl.path))


def get_full_url(url_part: str):
    return urljoin('https://czdap.icann.org', url_part)


def save_file(base_dir, sess, response):
    with open(get_output_filename(base_dir, response.url), 'wb') as f:
        for chunk in response.iter_content(4096):
            f.write(chunk)


def get_urls(token: str):
    if token is None or len(token) == 0 or re.match('[^a-zA-Z908]', token):
        raise ValueError('Invalid token format')

    r = requests.get(get_full_url('/user-zone-data-urls.json?token=' + token))

    if r.status_code != 200:
        raise RuntimeError(
            'Unexpected response (HTTP:{0}) from CZDAP, are you using the right token?'.format(r.status_code))
    try:
        return json.loads(r.text)
    except BaseException:
        raise ValueError('Unable to parse JSON response from CZDAP')


def main():
    arg_parser = ArgumentParser(description='Download CZDAP-API zonefiles in parallel')
    arg_parser.add_argument('-t', '--token', type=str, required=True,
                            help='Token provided by CZDAP website for API usage')
    arg_parser.add_argument('-O', '--output-dir', type=str, default='./zonefiles',
                            help='Output folder for downloaded zonefiles, default ./zonefiles')
    arg_parser.add_argument('-w', '--workers', type=int, default=3, help='Number of simultaneous request/save workers')
    args = arg_parser.parse_args()

    if not os.path.exists(args.output_dir):
        os.makedirs(args.output_dir)

    with FuturesSession(max_workers=args.workers) as session:
        futures = []
        for u in get_urls(args.token):
            save_func = partial(save_file, args.output_dir)
            futures.append(session.get(get_full_url(u), background_callback=save_func))

        success = 0
        errors = []

        for f in cf.as_completed(futures):
            try:
                f.result()
                success += 1
            except BaseException as ee:
                print(ee, file=sys.stderr)
                errors.append(ee)

        print('Successfully downloaded {} zonefiles, {} erorrs'.format(success, len(errors)))

    return


if __name__ == '__main__':
    sys.exit(main())