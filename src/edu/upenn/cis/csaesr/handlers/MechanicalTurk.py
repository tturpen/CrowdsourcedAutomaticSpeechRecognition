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
import os

class AssignmentHandler():
    def __init__(self,connection):
        self.conn = connection
        
    def approve_assignment(self,assignment_id,feedback=None):
        self.conn.approve_assignment(assignment_id, feedback)
        
    def reject_assignment(self,assignment_id,feedback=None):
        return self.conn.approve_assignment(assignment_id,feedback)

    def get_assignment(self,assignment_id,response_groups=None):
        return self.conn.get_assignment(assignment_id, response_groups)  
         
        
class TurkerHandler():
    def __init__(self,connection):
        self.conn = connection
        
    def block_worker(self,worker_id,reason):
        self.conn.block_worker(worker_id, reason)
    
    def un_block_worker(self,worker_id,reason):
        self.conn.un_block_worker(worker_id, reason)
    

class HitHandler():
    DEFAULT_DURATION = 60*5
    DEFAULT_REWARD = 0.05
    def __init__(self,connection,template_dir):
        self.conn = connection
        base_dir = os.getcwd()
        self.templates = {}
        self.html_tags = {"audio_url" : "${audiourl}",
                          "title" : "${title}",
                          "description" : "${description}"}
        self.html_head = open(os.path.join(template_dir,"transcriptionhead.html")).read()
        self.html_tail =  open(os.path.join(template_dir,"transcriptiontail.html")).read()
        self.html_question = open(os.path.join(template_dir,"transcriptionquestion.html")).read()
        self.templates["transcription"] = open(os.path.join(template_dir,"vanilla_transcription.html")).read()
                
    def dispose_HIT(self,hit_id):
        self.conn.dispose_hit(hit_id)       
    
    def get_HITs(self):
        return self.conn.get_all_hits()
    
    def get_HIT(self,hit_id,response_groups=None):
        return self.conn.get_hit(hit_id, response_groups)
    
    def make_html_transcription_HIT(self,audio_clip_urls,hit_title,question_title,description,keywords,
                               duration=DEFAULT_DURATION,reward=DEFAULT_REWARD):        
        print(self.conn.get_account_balance())
        
        overview = Overview()
        overview.append_field("Title", "Type the words in the following audio clip in order.")
        
        url = "http://www.cis.upenn.edu/~tturpen/basic_transcription_hit.html"
        description = "Transcribe the audio clip by typing the words that the person \
                        says in order."
                        
        keywords = "audio, transcription"
        html_head = self.html_head.replace(self.html_tags["title"],hit_title).replace(self.html_tags["description"],description)
        html = html_head
        for ac in audio_clip_urls:
            question = self.html_question.replace(self.html_tags["audio_url"],ac)
            html += question
        
        html += self.html_tail
        html_question = HTMLQuestion(html,800)
        return self.conn.create_hit(title=hit_title,
                                    question=html_question,
                                    max_assignments=1,
                                    description=description,
                                    keywords=keywords,
                                    duration = 60*5,
                                    reward = 0.02)
        
        
    def make_question_form_HIT(self,audio_clip_urls,hit_title,question_title,description,keywords,
                               duration=DEFAULT_DURATION,reward=DEFAULT_REWARD):
        overview = Overview()        
        overview.append_field("Title",hit_title)
        #overview.append(FormattedContent('<a target = "_blank" href="url">hyperlink</a>'))
        question_form = QuestionForm()
        question_form.append(overview)
        for ac in audio_clip_urls:
            audio_html = self.html_question.replace(self.audio_url_tag,ac)
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
            
            
            
            
            
            
            
            
            
            
            
            
            
