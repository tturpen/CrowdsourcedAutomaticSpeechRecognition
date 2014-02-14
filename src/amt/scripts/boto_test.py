from boto.mturk.connection import MTurkConnection
import os
HOST='mechanicalturk.sandbox.amazonaws.com'
aws_id = os.environ['AWS_ACCESS_KEY_ID']
aws_k = os.environ['AWS_SECRET_ACCESS_KEY']

mtc = MTurkConnection(aws_access_key_id=aws_id,
                      aws_secret_access_key=aws_k,
                      host=HOST)
print 'Made mt connection'
print mtc.get_account_balance()

from boto.mturk.question import ExternalQuestion, QuestionForm, Overview
overview = Overview()
overview.append_field("Title", "Type the words in the following audio clip in order")

url = "http://www.cis.upenn.edu/~tturpen/basic_transcription_hit.html"
description = "Transcribe the audio clip by typing the words that the person \
                says in order."
keywords = "audio, transcription"
eq = ExternalQuestion(url,400)
question_form = QuestionForm()
question_form.append(overview)
question_form.append(eq)

mtc.create_hit(questions=question_form,
               max_assignments=1,
               description=description,
               keywords=keywords,
               duration = 60*5,
               reward = 0.02)
