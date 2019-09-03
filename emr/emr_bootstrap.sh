#!/bin/bash -xe

sudo pip-3.6 install \
  pandas             \
  numpy              \
  nltk

sudo /usr/bin/python3 -m nltk.downloader -d /usr/share/nltk_data all