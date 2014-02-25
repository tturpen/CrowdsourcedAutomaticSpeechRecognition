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
from handlers.Audio import WavHandler, PromptHandler
from handlers.Exceptions import WavHandlerException, PromptNotFound
from util.calc import wer
from shutil import copyfile

import logging
import os

HOST='mechanicalturk.sandbox.amazonaws.com'
TEMPLATE_DIR = "/home/taylor/csaesr/src/resources/resources.templates/"
WER_THRESHOLD = 50

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
        self.wh = WavHandler()
        self.ph = PromptHandler()
        
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
                    self.mh.update_transcription_status(transcription,"Submitted")
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
            transcriptions = self.mh.get_transcriptions("audio_clip_id",[clip["_id"]])
            #for transcription in transcriptions:
            
    def assignment_submitted_approved(self):
        """For all submitted assignments,
            if an answered question has a reference transcription,
            check the WER.
            If all the answered questions with reference transcriptions
            have an acceptable WER, approve the assignment and update
            the audio clips and transcriptions."""
        assignments = self.mh.get_all_assignments_by_status("Submitted")
        approved = True
        for assignment in assignments:
            transcription_ids = assignment["transcriptions"]
            transcriptions = self.mh.get_transcriptions("_id",transcription_ids)
            for transcription in transcriptions:
                reference_id = self.mh.get_audio_clip({"_id":transcription["audio_clip_id"]},"reference")
                if reference_id:
                    reference_transcription = self.mh.get_reference_transcription({"_id": reference_id},
                                                                                  "transcription")
                    if reference_transcription:
                        transcription_wer = wer(reference_transcription,transcription["transcription"])
                        if transcription_wer < WER_THRESHOLD:
                            self.mh.update_transcription_status(transcription,"Confirmed")
                            logger.info("WER for transcription(%s) %d"%(transcription["transcription"],transcription_wer))
                        else:
                            approved = False
            if approved:
                self.mh.update_assignment_status(assignment,"Approved")                         
            print(transcription_ids)
            
    def audio_source_sourced_to_clipped(self):
        """For an audio source,
            re-sample/confirm sampling
            segment audio into clip/transcription pairs
            create audio clip and reference transcription artifacts."""
        pass
            
    def _bootstrap_rm_audio_source_file_to_sourced(self,file_dir,prompt_file_uri,
                                                   base_clip_dir,sample_rate=16000,
                                                   http_base_url = "http://www.cis.upenn.edu/~tturpen/wavs/"):
        """For an audio directory,
            see which files are new and not an audio source already
            """
        prompt_dict = self.ph.get_prompts(prompt_file_uri)
        for root, dirs, files in os.walk(file_dir):
            for f in files:
                system_uri = os.path.join(root,f)
                out_uri = system_uri.strip(".sph") + ".wav"
                out_uri = os.path.basename(out_uri)
                out_uri = os.path.join(root,(out_uri))
                spkr_id = str(os.path.relpath(root,file_dir))
                #sph to wav
                if not f.endswith(".wav") and not os.path.exists(out_uri):
                    try:
                        self.wh.sph_to_wav(system_uri,out_uri=out_uri)
                    except WavHandlerException as e:
                        logger.error("Unable to create wav from sph: "+str(e))
                        
                if os.path.exists(out_uri) and out_uri.endswith(".wav"):
                    #create audio source artifact
                    wav_filename = os.path.basename(out_uri)
                    prompt_id = os.path.basename(out_uri).strip(".wav").upper()
                    encoding = ".wav"
                    sample_rate = 16000
                    disk_space = os.stat(out_uri).st_size
                    length_seconds = self.wh.get_audio_length(out_uri)
                    if prompt_id in prompt_dict:                        
                        transcription_prompt = prompt_dict[prompt_id]
                    else:
                        #No prompt found
                        raise PromptNotFound
                    source_id = self.mh.create_audio_source_artifact(out_uri,
                                                         disk_space,
                                                         length_seconds,
                                                         sample_rate,
                                                         spkr_id,
                                                         encoding)
                    #create audio clip artifact
                    audio_clip_uri = os.path.join(base_clip_dir,spkr_id,wav_filename)                    
                    clip_dir = os.path.dirname(audio_clip_uri)
                    if not os.path.exists(clip_dir):
                        os.makedirs(clip_dir)
                    if not os.path.exists(audio_clip_uri):
                        copyfile(out_uri,audio_clip_uri)     
                    #http_url
                    http_url = os.path.join(http_base_url,spkr_id,wav_filename)                   
                    clip_id = self.mh.create_audio_clip_artifact(source_id,
                                                       0,
                                                       -1,
                                                       audio_clip_uri,
                                                       http_url,
                                                       length_seconds,
                                                       disk_space)
                    #TODO - tt create reference transcription from prompt
                    self.mh.create_reference_transcription_artifact()


            
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
    audio_file_dir = "/home/taylor/data/corpora/LDC/LDC93S3A/rm_comp/rm1_audio1/rm1/ind_trn"
    prompt_file_uri = "/home/taylor/data/corpora/LDC/LDC93S3A/rm_comp/rm1_audio1/rm1/doc/al_sents.snr"
    base_clip_dir = "/home/taylor/data/corpora/LDC/LDC93S3A/rm_comp/rm1_audio1/rm1/clips"
    #----------------------- selection = raw_input("""Please make a selection:\n
                                # 1: To create a HIT from the latest audioclip queue.
                                # 2: To list the current HITs (and delete them if desired.""")
    selection = "5"
    if selection == "1":
        tph.audio_clip_lifecycle(audio_clip_id)
    elif selection == "2":
        tph.allhits_liveness()
    elif selection == "3":
        tph.audio_clip_lifecycle_from_hit_to_submitted()
    elif selection == "4":
        tph.assignment_submitted_approved()
    elif selection == "5":
        tph._bootstrap_rm_audio_source_file_to_sourced(audio_file_dir,
                                                       prompt_file_uri,
                                                       base_clip_dir)
    



if __name__ == '__main__':
    main()