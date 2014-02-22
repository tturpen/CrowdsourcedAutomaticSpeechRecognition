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
from handlers.MongoDB import MongoHandler
from data.structures import TranscriptionHit

import logging
import os

HOST='mechanicalturk.sandbox.amazonaws.com'
TEMPLATE_DIR = "/home/taylor/csaesr/src/resources/resources.templates/"

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
    
    def audio_clip_lifecycle(self,audio_clip_id,priority=1,max_queue_size=3):    
        audio_clip_url = self.mh.get_audio_clip_url(audio_clip_id)
        status = self.mh.get_audio_clip_status(audio_clip_id)
        if status == "New":
            self.mh.queue_clip(audio_clip_id, priority, max_queue_size)
            response = self.audio_clip_lifecycle_from_queued_to_hit()
        elif status == "Queued":
            response = self.audio_clip_lifecycle_from_queued_to_hit()
        elif status == "Hit":
            print("In hit: %s"%audio_clip_url)

    
    def audio_clip_lifecycle_from_queued_to_hit(self):
        """Take queued audio clips from the audio clip queue
            put them in a hit and create the hit.
            If successful, update the audio clip status."""
        clip_queue = self.mh.get_audio_clip_queue()
        clip_pairs = self.mh.get_audio_clip_pairs(clip_queue)
        if clip_pairs:
            hit_title = "Audio Transcription"
            question_title = "List and Transcribe" 
            description = "Transcribe the audio clip by typing the words the person says in order."
            keywords = "audio, transcription, audio transcription"
            response = self.hh.make_html_transcription_HIT(clip_pairs,hit_title,
                                         question_title, description, keywords)
            if type(response) == ResultSet and len(response) == 1 and response[0].IsValid:
                response = response[0]
                self.mh.update_audio_clip_queue(clip_queue)
                audio_clip_ids = [w["audio_clip_id"] for w in clip_queue]    
                hit_id = response.HITId
                hit_type_id = response.HITTypeId
                self.mh.create_transcription_hit_document(hit_id,hit_type_id,clip_queue,"New")        
                logger.info("Successfully created HIT: %s"%hit_id)
                return self.mh.update_audio_clip_status(audio_clip_ids,"Hit")
            else:
                return False
            
    def audio_clip_lifecycle_from_hit_to_submitted(self):
        """Check all assignments for audio clip IDs.
            Update the audio clips."""
        hits = self.conn.get_all_hits()
        transcription_ids = []
        for hit in hits:
            hit_id = hit.HITId
            assignments = self.conn.get_assignments(hit_id)
            for assignment in assignments:                
                transcription_dicts = self.ah.get_assignment_submitted_transcriptions(assignment)

                for transcription in transcription_dicts:
                    self.mh.update_transcription(transcription,"Submitted")
                    self.mh.update_audio_clip_status([transcription["audio_clip_id"]], "Submitted")
                    transcription_ids.append(self.mh.get_transcription({"audio_clip_id" : transcription["audio_clip_id"],
                                                                        "assignment_id" : transcription["assignment_id"]},
                                                                       "_id"))
                self.mh.create_assignment_document(assignment,
                                                   transcription_ids,
                                                   "Submitted")
            if assignments:
                self.mh.update_transcription_hit_status(hit_id,"Submitted")
            print(transcription_dicts)
            
    def audio_clip_lifecycle_from_submitted_to_approved(self):
        """TODO tt- I got a little ahead of myself, need to implement AssignmentSubmittedApproved first
            For all the submitted audio clips, if an audio clip
            has a transcription, check the reference transcription,
            if the WER is acceptable, update the audio clip status"""
        audio_clips = self.mh.get_all_audio_clips_by_status("Submitted")
        for clip in audio_clips:
            reference_transcription = clip["reference_transcription"]
            transcriptions = self.mh.get_transcriptions(clip["_id"])
            #for transcription in transcriptions:
            
    def assignment_submitted_approved(self):
        """For all submitted assignments,
            if an answered question has a reference transcription,
            check the WER.
            If all the answered questions with reference transcriptions
            have an acceptable WER, approve the assignment and update
            the audio clips and transcriptions."""
        assignments = self.mh.get_all_assignments_by_status("Submitted")
        for assignment in assignments:
            transcription_ids = assignment["transcriptions"]
            print(transcription_ids)
                
        
                

            
            
    def allhits_liveness(self):
        #allassignments = self.conn.get_assignments(hit_id)
        #first = self.ah.get_submitted_transcriptions(hit_id,str(clipid))

        hits = self.conn.get_all_hits()
        clip_id = "12345"
        for hit in hits:
            hit_id = hit.HITId
            print("Submitted transcriptions for %d given hitID: %s "%(clip_id,hit_id))
            print(hit)
            if raw_input("Remove hit?(y/n)") == "y":
                try:
                    self.conn.disable_hit(hit.HITId)
                    self.mh.remove_transcription_hit(hit_id)
                except MTurkRequestError as e:
                    raise e
        
#------------------------------------------------------------------------------ 
            #-------------------------------- allhits = self.conn.get_all_hits()
#------------------------------------------------------------------------------ 
            #------------------------------------------------- expired_hits = []
            #----------------------------------------------- available_hits = []
    


    
def main():
    audio_clip_id = 12345
    tph = TranscriptionPipelineHandler()
    #----------------------- selection = raw_input("""Please make a selection:\n
                                # 1: To create a HIT from the latest audioclip queue.
                                # 2: To list the current HITs (and delete them if desired.""")
    selection = "3"
    if selection == "1":
        tph.audio_clip_lifecycle(audio_clip_id)
    elif selection == "2":
        tph.allhits_liveness()
    elif selection == "3":
        tph.audio_clip_lifecycle_from_hit_to_submitted()
    elif selection == "4":
        tph.assignment_submitted_approved()
    



if __name__ == '__main__':
    main()