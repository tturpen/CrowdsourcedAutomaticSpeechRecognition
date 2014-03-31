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

HOST='mechanicalturk.amazonaws.com'
#HOST='mechanicalturk.sandbox.amazonaws.com'
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
    #Switch engine from here
    elicitation_engine()
    #transcription_engine()
    
def elicitation_engine():
    eph = ElicitationPipelineHandler()
    eph.run()

    
def transcription_engine():    
    tph = TranscriptionPipelineHandler()
    tph.run()
    
    



if __name__ == '__main__':
    main()