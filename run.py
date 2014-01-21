#!/usr/bin/env python
# -*- coding: utf-8 -*-

import m3u8
import threading
import logging
from time import sleep

# Use a static URL during initial development
URI = 'http://www.nasa.gov/multimedia/nasatv/NTV-Public-IPS.m3u8'

logger = logging.getLogger('digest')
logger.setLevel(logging.DEBUG)
# create file handler which logs even debug messages
fh = logging.FileHandler('spam.log')
fh.setLevel(logging.WARNING)
# create console handler with a higher log level
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
# create formatter and add it to the handlers
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(threadName)s %(message)s')
fh.setFormatter(formatter)
ch.setFormatter(formatter)
# add the handlers to the logger
logger.addHandler(fh)
logger.addHandler(ch)

def worker(playlist, m3u8_uri):
    '''the thread worker function'''
    logger.debug("worker for playlist: %s" % m3u8_uri)
    playlist_loop(playlist, m3u8_uri)
    return

def load_playlist(m3u8_uri):
    ''' load m3u8 URI and extract playlists '''
    logger.debug('Load playlist %s' % m3u8_uri)
    playlist = m3u8.load(m3u8_uri)
    if playlist.is_variant:
        return digest_variant_playlist(playlist, m3u8_uri)
    else:
        return digest_singel_playlist(playlist, m3u8_uri)

def digest_variant_playlist(playlist, m3u8_uri):
    ''' extract single playlists from variant '''
    for p in playlist.playlists:
        load_playlist(p.absolute_uri)

def digest_singel_playlist(playlist, m3u8_uri):
    '''launch single playlist to a worker'''
    #TODO: add thread name (name=??, target=worker, args=(playlist,))
    t = threading.Thread(target=worker, args=(playlist, m3u8_uri,))
    t.setDaemon(True)
    threads.append(t)
    t.start()

def playlist_loop(playlist, m3u8_uri):
    sequence = 0
    while True:
        if sequence == playlist.media_sequence:
            duration = playlist.target_duration
            logger.debug('sleeping for %s seconds' % duration)
            sleep(duration)
            playlist = m3u8.load(m3u8_uri)
        else:
            logger.info('sequence: %s' % sequence)
            playlist = m3u8.load(m3u8_uri)
            #poke_segment(playlist)
            sequence = playlist.media_sequence

def poke_segment(playlist):
    ''' poke segments, if error return segment and error message '''
    uris = [segment.absolute_uri for segment in playlist.segments]
    logger.debug(uris)

if __name__ == "__main__":
    threads = []
    load_playlist(URI)
    while True:
        sleep(1)

