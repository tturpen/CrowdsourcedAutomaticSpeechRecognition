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
from boto.mturk.connection import MTurkConnection, MTurkRequestError
from boto.mturk.question import ExternalQuestion, QuestionForm, Overview, HTMLQuestion, QuestionContent, FormattedContent, FreeTextAnswer, AnswerSpecification, Question
from handlers.Exceptions import IncorrectTextFieldCount
import os
import logging

class AssignmentHandler():
    def __init__(self,connection):
        self.conn = connection
        self.logger = logging.getLogger("transcription_engine.mechanicalturk_handler")
        
    def approve_assignment(self,assignment_id,feedback=None):
        self.conn.approve_assignment(assignment_id, feedback)
        
    def reject_assignment(self,assignment_id,feedback=None):
        return self.conn.approve_assignment(assignment_id,feedback)

    def get_assignment(self,assignment_id,response_groups=None):
        return self.conn.get_assignment(assignment_id, response_groups)  
    
    def get_submitted_transcriptions(self,hit_id,audio_clip_id):
        """Given the hit_id and the audio clip id, find all transcriptions
            in the submitted assignments"""
        allassignments = self.conn.get_assignments(hit_id)
        response = []
        for assignment in allassignments:
            for result_set in assignment.answers:
                for question_form_answer in result_set:
                    if question_form_answer.qid == audio_clip_id:
                        if len(question_form_answer.fields) != 1:
                            raise IncorrectTextFieldCount
                        response.append(question_form_answer.fields[0])
        self.logger.info("Retrieved transcription(%s) for audio clip(%s)"%(response,audio_clip_id))
        return response
    
    def get_all_submitted_transcriptions(self,hit_id):
        """Given the hit_id find all transcriptions
            in the submitted assignments."""
        allassignments = self.conn.get_assignments(hit_id)
        response = []
        assignment_ids = []
        for assignment in allassignments:
            assignment_ids.append(assignment.AssignmentId)
            response.extend(self.get_assignment_submitted_transcriptions(assignment))
        return response
    
    def get_assignment_submitted_transcriptions(self,assignment):
        """Given the assignment return all the transcriptions."""
        response = []
        for result_set in assignment.answers:
            for question_form_answer in result_set:
                if len(question_form_answer.fields) != 1:
                    raise IncorrectTextFieldCount
                response.append({"audio_clip_id": question_form_answer.qid,
                                 "assignment_id": assignment.AssignmentId,
                                 "transcription": question_form_answer.fields[0],                                    
                                 "worker_id" : assignment.WorkerId,
                                 })
        self.logger.info("Retrieved transcriptions for assignment(%s)"%(assignment))
        return response
    
class TurkerHandler():
    def __init__(self,connection):
        self.conn = connection
        
    def block_worker(self,worker_id,reason):
        self.conn.block_worker(worker_id, reason)
    
    def un_block_worker(self,worker_id,reason):
        self.conn.un_block_worker(worker_id, reason)
    

class HitHandler():
    DEFAULT_DURATION = 60*5
    DEFAULT_REWARD = 0.02
    DEFAULT_MAX_ASSIGNMENTS = 3
    def __init__(self,connection,template_dir):
        self.conn = connection
        base_dir = os.getcwd()
        self.templates = {}
        self.html_tags = {"audio_url" : "${audiourl}",
                          "title" : "${title}",
                          "description" : "${description}",
                          "audioclip_id" : "${audioclipid}",
                          "prompt" : "${prompt}",
                          "prompt_id": "${promptid}",
                          "disable_script" : "${disable_script}",
                          "audio_id" : "${audio_id}"}
        
        #Transcription html templates
        self.transcription_head = open(os.path.join(template_dir,"transcriptionhead.html")).read()
        self.transcription_tail =  open(os.path.join(template_dir,"transcriptiontail.html")).read()
        self.transcription_question = open(os.path.join(template_dir,"transcriptionquestion.html")).read()
        
        #Elicitation html templates
        self.transcription_head = open(os.path.join(template_dir,"elicitationhead.html")).read()
        self.transcription_tail =  open(os.path.join(template_dir,"elicitationtail.html")).read()
        self.templates["transcription"] = open(os.path.join(template_dir,"vanilla_transcription.html")).read()
        
        self.disable_input_script = 'document.getElementById("${input_id}").disabled = true;'
                
    def dispose_HIT(self,hit_id):
        self.conn.dispose_hit(hit_id)       
    
    def get_HITs(self):
        return self.conn.get_all_hits()
    
    def get_HIT(self,hit_id,response_groups=None):
        return self.conn.get_hit(hit_id, response_groups)
    
    def estimate_html_HIT_cost(self,audio_clip_urls,reward_per_clip,
                               max_assignments):
        return reward_per_clip * len(audio_clip_urls) * max_assignments  
    
    def make_html_elicitation_HIT(self,prompt_list,hit_title,prompt_title,description,keywords,
                                  duration=DEFAULT_DURATION,reward_per_clip=DEFAULT_REWARD,max_assignments=DEFAULT_MAX_ASSIGNMENTS):
        overview = Overview()
        overview.append_field("Title", "Record yourself speaking the words in the prompt.")
        descriptions = ["The following prompts are in English.",
                        "Click the red circle to record yourself.",
                        "Play the clip back to verify sound quality."
                        ]
        keywords = "audio, recording, elicitation, English"
        
        html_head = self.elicitation_head.replace(self.html_tags["title"],hit_title)
        for description in descriptions:            
            html_head = html_head.replace(self.html_tags["description"],
                                          "<li>"+description+"</li>\n"+self.html_tags["description"])    
        questions_html = []
        prompt_ids = []
        
        for prompt_words,prompt_id in prompt_list:
            #For each prompt, generate the question html given the template
            prompt_id = str(prompt_id) 
            prompt = " ".join(prompt_words)
            question = self.elicitation_question.replace(self.html_tags["prompt"],prompt)
            question = question.replace(self.html_tags["prompt_id"],str(prompt_id))
            questions_html.append(question)
            prompt_ids.append(prompt_id)
            
        for prompt_id in prompt_ids:
            #Disable the inputs for the prompts, which are just text fields for the 
            #audio recording URLs
            script = self.disable_input_script.replace("${input_id}",prompt_id)
            html_head = html_head.replace(self.html_tags["disable_script"],script+\
                                          "\n"+self.html_tags["disable_script"])
            if(self.html_tags["prompt_id"]) in html_head:
                html_head = html_head.replace(self.html_tags["prompt_id"],"'"+prompt_id+"'"+\
                                              ","+self.html_tags["prompt_id"])
        #Get rid of html tags
        html_head = html_head.replace(self.html_tags["disable_script"],"")
        html_head = html_head.replace(","+self.html_tags["prompt_id"],"")
        html_head = html_head.replace(self.html_tags["description"],"")
        html = html_head

        for question in questions_html:        
            html += question
        
        html += self.transcription_tail
        html_question = HTMLQuestion(html,800)
        
        #reward calculation
        reward = reward_per_clip*len(prompt_list)
#         try:
#             return self.conn.create_hit(title=hit_title,
#                                     question=html_question,
#                                     max_assignments=max_assignments,
#                                     description=description,
#                                     keywords=keywords,
#                                     duration = duration,
#                                     reward = reward)
#         except MTurkRequestError as e:
#             if e.reason != "OK":
#                 raise 
#             else:
#                 print(e) 
#                 return False
        return False
    
    def make_html_transcription_HIT(self,audio_clip_urls,hit_title,question_title,description,keywords,
                               duration=DEFAULT_DURATION,reward_per_clip=DEFAULT_REWARD,max_assignments=DEFAULT_MAX_ASSIGNMENTS):        
        overview = Overview()
        overview.append_field("Title", "Type the words in the following audio clip in order.")
        
        descriptions = ["The following audio clips are in English.",
                        "Transcribe the audio clip by typing the words that the person \
                        says in order.",
                        "Do not use abbreviations: 'street' and NOT 'st.'",
                        "Write numbers long-form, as in: 'twenty fifth' NOT '25th'.",
                        "Write letters (see example).",
                        "Punctuation does not matter.",
                        "Hotkeys: press Tab to play the next clip."]
                        
        keywords = "audio, transcription, English"
        html_head = self.transcription_head.replace(self.html_tags["title"],hit_title)
        for description in descriptions:            
            html_head = html_head.replace(self.html_tags["description"],
                                          "<li>"+description+"</li>\n"+self.html_tags["description"])
        count = 0
        questions = []
        inputs = []
        for acurl,acid in audio_clip_urls:
            input_id = str(acid) 
            question = self.transcription_question.replace(self.html_tags["audio_url"],acurl)
            question = question.replace(self.html_tags["audioclip_id"],str(acid))
            question = question.replace("${count}",input_id)
            count += 1
            questions.append(question)
            inputs.append(input_id)
            
        for input_id in inputs:
            script = self.disable_input_script.replace("${input_id}",input_id)
            html_head = html_head.replace(self.html_tags["disable_script"],script+\
                                          "\n"+self.html_tags["disable_script"])
            if(self.html_tags["audio_id"]) in html_head:
                html_head = html_head.replace(self.html_tags["audio_id"],"'"+\
                                              input_id+"'"+","+self.html_tags["audio_id"])
            
        html_head = html_head.replace(self.html_tags["disable_script"],"")
        html_head = html_head.replace(","+self.html_tags["audio_id"],"")
        html_head = html_head.replace(self.html_tags["description"],"")
        html = html_head

        for question in questions:        
            html += question
            count += 1
        
        html += self.transcription_tail
        html_question = HTMLQuestion(html,800)
        
        #reward calculation
        reward = reward_per_clip*len(audio_clip_urls)
        try:
            return self.conn.create_hit(title=hit_title,
                                    question=html_question,
                                    max_assignments=max_assignments,
                                    description=description,
                                    keywords=keywords,
                                    duration = duration,
                                    reward = reward)
        except MTurkRequestError as e:
            if e.reason != "OK":
                raise 
            else:
                print(e) 
                return False
        return False
        
        
    def make_question_form_HIT(self,audio_clip_urls,hit_title,question_title,description,keywords,
                               duration=DEFAULT_DURATION,reward=DEFAULT_REWARD):
        overview = Overview()        
        overview.append_field("Title",hit_title)
        #overview.append(FormattedContent('<a target = "_blank" href="url">hyperlink</a>'))
        question_form = QuestionForm()
        question_form.append(overview)
        for ac in audio_clip_urls:
            audio_html = self.transcription_question.replace(self.audio_url_tag,ac)
            qc = QuestionContent()
            qc.append_field("Title",question_title)            
            qc.append(FormattedContent(audio_html))
            fta = FreeTextAnswer()
            q = Question(identifier="transcription",
                         content=qc,
                         answer_spec=AnswerSpecification(fta))
            question_form.append(q)
        try:
            response = self.conn.create_hit(questions=question_form,
                             max_assignments=1,
                             title=hit_title,
                             description=description,
                             keywords=keywords,
                             duration=duration,
                             reward=reward)
        except MTurkRequestError as e:
            if e.reason != "OK":
                raise 

        return question_form, response
            
            
            
            
            
            
            
            
            
            
            
            
            
