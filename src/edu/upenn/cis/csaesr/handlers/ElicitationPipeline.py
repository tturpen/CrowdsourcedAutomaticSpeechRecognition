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
from boto.mturk.connection import MTurkConnection, MTurkRequestError, ResultSet
from handlers.MechanicalTurk import AssignmentHandler, TurkerHandler, HitHandler
from handlers.MongoDB import MongoElicitationHandler

from handlers.Audio import WavHandler, PromptHandler
from handlers.Exceptions import WavHandlerException, PromptNotFound
from filtering.StandardFilter import Filter
from util.calc import cer_wer
from shutil import copyfile

import logging
import os

#HOST='mechanicalturk.amazonaws.com'
HOST='mechanicalturk.sandbox.amazonaws.com'
TEMPLATE_DIR = "/home/taylor/csaesr/src/resources/resources.templates/"

class ElicitationPipelineHandler(object):

    def __init__(self):
        aws_id = os.environ['AWS_ACCESS_KEY_ID']
        aws_k = os.environ['AWS_ACCESS_KEY']

        self.conn = MTurkConnection(aws_access_key_id=aws_id,\
                          aws_secret_access_key=aws_k,\
                          host=HOST)
        
        self.ah = AssignmentHandler(self.conn)
        self.th = TurkerHandler(self.conn)
        self.hh = HitHandler(self.conn,TEMPLATE_DIR)
        self.mh = MongoElicitationHandler()
        self.ph = PromptHandler()
        self.filter = Filter(self.mh)
        self.balance = self.conn.get_account_balance()[0].amount
        self.logger = logging.getLogger("transcription_engine.elicitation_pipeline_handler")
        
    def load_PromptSource_RawToList(self,prompt_file_uri):
        """Create the prompt artifacts from the source."""        
        prompt_dict = self.ph.get_prompts(prompt_file_uri)
        
    def run(self):
        audio_file_dir = "/home/taylor/data/corpora/LDC/LDC93S3A/rm_comp/rm1_audio1/rm1/ind_trn"
        #audio_file_dir = "/home/taylor/data/corpora/LDC/LDC93S3A/rm_comp/rm1_audio1/rm1/dep_trn"
        prompt_file_uri = "/home/taylor/data/corpora/LDC/LDC93S3A/rm_comp/rm1_audio1/rm1/doc/al_sents.snr"
        base_clip_dir = "/home/taylor/data/corpora/LDC/LDC93S3A/rm_comp/rm1_audio1/rm1/clips"
        selection = 0
        init_clip_count = 10000
        while selection != "12":
            selection = raw_input("""Prompt Source raw to Elicitations-Approved Pipeline:\n
                                     1: PromptSource-Load_RawToList: Load Resource Management 1 prompt source files to %d queueable(List) prompts
                                     2: Prompt-ReferencedToHit: Queue all referenced prompts and create a HIT if the queue is full.
                                     3: Prompt-HitToAssignmentSubmitted: Check all submitted assignments for Elicitations.
                                     4: PromptAssignment-SubmittedToElicitation: Check all submitted assignments for Elicitations.
                                     5: Elicitation-NewToSecondPassQueue: Check all submitted clips against their reference.
                                     6: Elicitation-NewToApproved: Check all submitted clips against their reference.
                                     7: Review Current Hits
                                     8: Worker liveness
                                     9: Account balance
                                     10: Worker stats
                                     11: Recalculate worker WER
                                     12: Exit
                                    """%init_clip_count)
            if selection == "1":
                self.load_PromptSource_RawToList(prompt_file_uri)
            else:
                selection = "12"
                
#    prompt_dict = self.ph.get_prompts(prompt_file_uri)