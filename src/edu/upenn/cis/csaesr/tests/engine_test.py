# Copyright (C) 2014 Taylor Turpen
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
from boto.mturk.connection import MTurkConnection, HIT, MTurkRequestError, ResultSet
from boto.mturk.question import ExternalQuestion, QuestionForm, Overview
from handlers.MechanicalTurk import AssignmentHandler, TurkerHandler, HitHandler
from handlers.MongoDB import MongoTranscriptionHandler
from handlers.TranscriptionPipeline import TranscriptionPipelineHandler
from handlers.ElicitationPipeline import ElicitationPipelineHandler
from data.structures import TranscriptionHit
from handlers.Audio import WavHandler, PromptHandler
from handlers.Exceptions import WavHandlerException, PromptNotFound
from filtering.StandardFilter import Filter
from util.calc import wer, cer_wer
from shutil import copyfile

import logging
import os

#HOST='mechanicalturk.amazonaws.com'
HOST='mechanicalturk.sandbox.amazonaws.com'
TEMPLATE_DIR = "/home/taylor/csaesr/src/resources/resources.templates/"
WER_THRESHOLD = .33

#Init Logger
logger = logging.getLogger("transcription_engine")
logger.setLevel(logging.DEBUG)
#write logs to file
fh = logging.FileHandler("engine_test.log")
fh.setLevel(logging.DEBUG)
#console handler to write to console
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
#format for logging
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
ch.setFormatter(formatter)
#Add the handlers to the logger
logger.addHandler(fh)
logger.addHandler(ch)
 

                
        
#------------------------------------------------------------------------------ 
            #-------------------------------- allhits = self.conn.get_all_hits()
#------------------------------------------------------------------------------ 
            #------------------------------------------------- expired_hits = []
            #----------------------------------------------- available_hits = []
    


def main():
    elicitation_engine()
    #transcription_engine()
    
def elicitation_engine():
    eph = ElicitationPipelineHandler()
    eph.run()

    
def transcription_engine():    
    tph = TranscriptionPipelineHandler()
    audio_file_dir = "/home/taylor/data/corpora/LDC/LDC93S3A/rm_comp/rm1_audio1/rm1/ind_trn"
    #audio_file_dir = "/home/taylor/data/corpora/LDC/LDC93S3A/rm_comp/rm1_audio1/rm1/dep_trn"
    prompt_file_uri = "/home/taylor/data/corpora/LDC/LDC93S3A/rm_comp/rm1_audio1/rm1/doc/al_sents.snr"
    base_clip_dir = "/home/taylor/data/corpora/LDC/LDC93S3A/rm_comp/rm1_audio1/rm1/clips"
    selection = 0
    init_clip_count = 10000
    while selection != "10":
        selection = raw_input("""Audio Source file to Audio Clip Approved Pipeline:\n
                                 1: AudioSource-FileToClipped: Initialize Resource Management audio source files to %d queueable(Referenced) clips
                                 2: AudioClip-ReferencedToHit: Queue all referenced audio clips and create a HIT if the queue is full.
                                 3: AudioClip-HitToSubmitted: Check all submitted assignments for Transcriptions.
                                 4: AudioClip-SubmittedToApproved: Check all submitted clips against their reference.
                                 5: Review Current Hits
                                 6: Worker liveness
                                 7: Account balance
                                 8: Worker stats
                                 9: Recalculate worker WER
                                 10: Exit
                                """%init_clip_count)
        #selection = "5"
        if selection == "1":
            tph._load_rm_audio_source_file_to_clipped(audio_file_dir,
                                                   prompt_file_uri,
                                                   base_clip_dir,init_clip_count=init_clip_count)
        elif selection == "2":
            tph.audio_clip_referenced_to_hit()
        elif selection == "3":
            tph.load_assignments_hit_to_submitted()
        elif selection == "4":
            tph.assignment_submitted_approved()
        elif selection == "5":
            tph.allhits_liveness()
        elif selection == "6":
            tph.all_workers_liveness()
        elif selection == "7":
            print("Account balance: %s"%tph.balance)
        elif selection == "8":
            tph.stats()
        elif selection == "9":
            tph.recalculate_worker_assignment_wer()
    



if __name__ == '__main__':
    main()