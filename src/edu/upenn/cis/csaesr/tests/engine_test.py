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
from boto.mturk.connection import MTurkConnection, HIT, MTurkRequestError
from boto.mturk.question import ExternalQuestion, QuestionForm, Overview
from handlers.MechanicalTurk import AssignmentHandler, TurkerHandler, HitHandler
from handlers.MongoDB import MongoHandler
from data.structures import TranscriptionHit

import os
HOST='mechanicalturk.sandbox.amazonaws.com'
TEMPLATE_DIR = "/home/taylor/csaesr/src/resources/resources.templates/"
class TranscriptionPipelineHandler():
    def __init__(self):
        aws_id = os.environ['AWS_ACCESS_KEY_ID']
        aws_k = os.environ['AWS_ACCESS_KEY']

        self.conn = MTurkConnection(aws_access_key_id=aws_id,\
                          aws_secret_access_key=aws_k,\
                          host=HOST)
        
        self.ah = AssignmentHandler(self.conn)
        self.th = TurkerHandler(self.conn)
        self.hh = HitHandler(self.conn,TEMPLATE_DIR)
        self.mh = MongoHandler()
        
    def generate_audio_HITs(self):
        pass   
    
def main():
    hit_title = "Audio Transcription"
    question_title = "List and Transcribe" 
    description = "Transcribe the audio clip by typing the words the person says in order."
    keywords = "audio, transcription, audio transcription"
    tph = TranscriptionPipelineHandler()    
    allhits = tph.conn.get_all_hits()

    expired_hits = []
    available_hits = []
    
    #tph.hh.make_transcription_HIT("http://www.cis.upenn.edu/~tturpen/wavs/testwav.wav")   
    audio_pairs = [("http://www.cis.upenn.edu/~tturpen/wavs/testwav.wav",12345),
                   ("http://www.cis.upenn.edu/~tturpen/wavs/testwav.wav",54321)]
    #===========================================================================
    # tph.hh.make_html_transcription_HIT(audio_pairs,hit_title,
    #                              question_title, description, keywords)
    #===========================================================================
    #===========================================================================
    # tph.hh.make_question_form_HIT(["http://www.cis.upenn.edu/~tturpen/wavs/testwav.wav"],hit_title,
    #                              question_title, description, keywords)
    #===========================================================================
    
    for hit in allhits:
        allassignments = tph.conn.get_assignments(hit.HITId)
        first = tph.ah.get_submitted_transcriptions(hit.HITId,str(12345))
        print("Submitted transcriptions for %d: %s given hitID: %s "%(12345,first,hit.HITId))

        if hit.HITStatus == "Assignable":
            available_hits.append(TranscriptionHit(hit))
        if raw_input("Remove hit?(y/n)") == "y":
            try:
                tph.conn.disable_hit(hit.HITId)
            except MTurkRequestError as e:
                if e.reason != "OK":
                    raise 
           
            
    for hit in available_hits:
        print(hit.toString())
        



if __name__ == '__main__':
    main()