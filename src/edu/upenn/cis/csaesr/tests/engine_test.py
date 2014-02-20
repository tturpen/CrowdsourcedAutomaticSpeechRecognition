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
    
    def audio_clip_lifecycle(self,audio_clip_id):    
        audio_clip_url = self.mh.get_audio_clip_url(audio_clip_id)
        status = self.mh.get_audio_clip_status(audio_clip_id)
        if status == "New":
            response = self.transcription_hit_lifecycle(audio_clip_url)
            self.mh.update_audio_clip_status(audio_clip_url,response)
    
    def transcription_hit_lifecycle_from_new(self,audio_clip_url):
        #Queue is the audio clip and 
        clip_pairs = self.mh.get_audio_clip_queue(audio_clip_url)
        if clip_pairs:
            hit_title = "Audio Transcription"
            question_title = "List and Transcribe" 
            description = "Transcribe the audio clip by typing the words the person says in order."
            keywords = "audio, transcription, audio transcription"
        
            allhits = self.conn.get_all_hits()
        
            expired_hits = []
            available_hits = []
    
            response = self.hh.make_html_transcription_HIT(clip_pairs,hit_title,
                                         question_title, description, keywords)
            if response:
                self.mh.update_audio_clip_queue(clip_pairs)
            else:
                return False
            
    def transcription_hit_lifecycle_from_assigned(self,hit_id):
        allassignments = self.conn.get_assignments(hit_id)
        #------ first = self.ah.get_submitted_transcriptions(hit_id,str(clipid))
        # print("Submitted transcriptions for %d: %s given hitID: %s "%(clipid,first,hit_id))
#------------------------------------------------------------------------------ 
        #------------------------------------- if hit.HITStatus == "Assignable":
            #---------------------- available_hits.append(TranscriptionHit(hit))
        #------------------------------ if raw_input("Remove hit?(y/n)") == "y":
            #-------------------------------------------------------------- try:
                #------------------------------ self.conn.disable_hit(hit.HITId)
            #------------------------------------ except MTurkRequestError as e:
                #------------------------------------------ if e.reason != "OK":
                    #----------------------------------------------------- raise
#------------------------------------------------------------------------------ 
#------------------------------------------------------------------------------ 
            #---------------------------------------- for hit in available_hits:
                #----------------------------------------- print(hit.toString())
        


    
def main():
    audio_clip_id = 12345
    tph = TranscriptionPipelineHandler()
    tph.audio_clip_lifecycle(audio_clip_id)
    



if __name__ == '__main__':
    main()