CrowdsourcedAutomaticSpeechRecognition
======================================

Dependencies:
python:
boto
pymongo

linux:
mongodb

Introduction
------------
The two main classes in this project are src/edu/upenn/cis/csaesr/handlers/TranscriptionHandler.py 
and src/edu/upenn/cis/csaesr/handlers/ElicitationHandler.py

Both pipeline handlers have a "run" function that takes care of loading source files, creating HITs, loading submitted
assignments from MTurk and approving the hits.

