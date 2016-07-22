#!/usr/bin/env python3

import os
from ftplib import FTP
from datetime import datetime
from subprocess import check_call
from glob import glob
import argparse
import re
import requests


def download_laads(date, data_dir, earthdata_username, earthdata_password,
                   ladssci_username, ladssci_password):
    """Download LAADS data for a given date in order
    to allow the preprocessing of a Landsat 8 scene with LaSRC.
    """
    # Convert date to datetime if needed
    if not isinstance(date, datetime):
        date = datetime.strptime(date, '%Y-%m-%d')

    def _download_cmg(date, data_dir, username, password):
        """Download CMG products."""
        date = date.strftime('%Y.%m.%d')
        base_url = 'http://e4ftl01.cr.usgs.gov'
        mod09 = 'MOLT/MOD09CMG.006'
        myd09 = 'MOLA/MYD09CMG.006'
        for directory in [mod09, myd09]:
            product = directory[5:13]
            dl_page_url = '/'.join([base_url, directory, date])
            dl_page = requests.get(dl_page_url).text
            regex = re.compile('%s.{27}\.hdf' % product)
            remote_fn = regex.search(dl_page).group(0)
            dl_url = '/'.join([dl_page_url, remote_fn])
            filename = os.path.join(data_dir, remote_fn)
            check_call(['wget', '--user', username, '--password',
                        password, '-O', filename, dl_url])

    def _download_cma(date, data_dir, username, password):
        """Download CMA products."""
        year = date.year
        julian_day = (date - datetime(year, 1, 1)).days + 1
        base_url = 'ftp://ladssci.nascom.nasa.gov'
        ladssci = FTP(base_url.split('/')[-1], user=username, passwd=password)
        for product in ['MOD09CMA', 'MYD09CMA']:
            directory = '/'.join(['/6', product, str(year),
                                  '%03d' % julian_day])
            ladssci.cwd(directory)
            remote_fn = ladssci.nlst()[0]
            dl_url = '/'.join([base_url, directory, remote_fn])
            check_call(['wget', '--user', username, '--password', password,
                        '-P', data_dir, dl_url])

    # Download CMA and CMG products
    _download_cmg(date, data_dir, earthdata_username, earthdata_password)
    _download_cma(date, data_dir, ladssci_username, ladssci_password)

    # Fuse products and delete old files
    os.chdir(data_dir)
    date_id = str(date.year) + str((date - datetime(date.year, 1, 1)).days + 1)
    terra_cmg = glob('MOD09CMG*{0}*.hdf'.format(date_id))[0]
    terra_cma = glob('MOD09CMA*{0}*.hdf'.format(date_id))[0]
    aqua_cma = glob('MYD09CMA*{0}*.hdf'.format(date_id))[0]
    aqua_cmg = glob('MYD09CMG*{0}*.hdf'.format(date_id))[0]
    check_call(['combine_l8_aux_data', '--terra_cmg', terra_cmg,
                '--terra_cma', terra_cma, '--aqua_cmg', aqua_cmg,
                '--aqua_cma', aqua_cma, '--output_dir', data_dir])
    for old in [terra_cmg, terra_cma, aqua_cma, aqua_cmg]:
        os.remove(old)

parser = argparse.ArgumentParser(
    description='Download LAADS data for a given date.')
parser.add_argument('--date', help='Format: YYYY-MM-DD')
parser.add_argument('--euser', help='EarthData username')
parser.add_argument('--epass', help='EarthData password')
parser.add_argument('--luser', help='Ladssci username')
parser.add_argument('--lpass', help='Ladssci password')

if __name__ == '__main__':
    args = parser.parse_args()
    L8_AUX_DIR = os.environ['L8_AUX_DIR']
    OUTPUT_DIR = os.path.join(L8_AUX_DIR, 'LADS')
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    download_laads(args.date, OUTPUT_DIR, args.euser, args.epass,
                   args.luser, args.lpass)
