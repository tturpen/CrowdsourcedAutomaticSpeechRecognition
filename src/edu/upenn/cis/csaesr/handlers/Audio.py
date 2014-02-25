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
from subprocess import call, check_output
from handlers.Exceptions import WavHandlerException, DuplicateSentenceIds
import os
class WavHandler(object):
    """This class is for handling wav encoded files.""" 
    DEFAULT_SAMPLE_RATE = 16000
    SPH2PIPE_SYS_BINARY = "/home/taylor/repos/kaldi-stable/kaldi-stable/tools/sph2pipe_v2.5/sph2pipe"

    def __init__(self):
        self.sox_handler = SoxHandler()
        
    def sph_to_wav(self,system_uri,wav_dir=None,out_uri=None):
        """Given an sph encoded audio file, convert it to wav."""
        if not wav_dir:
            wav_dir = os.path.dirname(system_uri)
        if not out_uri:
            out_uri = system_uri.strip(".sph") + ".wav"
            out_uri = os.path.basename(out_uri)
            out_uri = os.path.join(wav_dir,(out_uri))
        out_file = open(out_uri,"w")
        try:
            call([self.SPH2PIPE_SYS_BINARY,"-f", "wav",system_uri],stdout=out_file)
        except Exception:
            raise WavHandlerException
        out_file.close()
        
    def get_audio_length(self,system_uri,encoding=".wav"):
        if(encoding == ".wav"):
            return self.sox_handler.get_wav_audio_length(system_uri)
        
class PromptHandler(object):
    def __init__(self):
        pass
    
    def get_prompts(self,prompt_file_uri,comment_char=";"):
        lines = open(prompt_file_uri).readlines()
        result = {}
        for line in lines:
            if not line.startswith(";"):
                words = line.split(" ")
                sent_id = words[-1].strip().lstrip("(").strip(")")
                if sent_id in result:
                    raise DuplicateSentenceIds
                result[sent_id] = words[:-1]
        return result
               
        
class SoxHandler(object):
    """This class is an interface to the Sox audio file handler"""
    SOXI_BINARY = "soxi"
    def get_wav_audio_length(self,system_uri):
        length = float(check_output([self.SOXI_BINARY,"-D",system_uri]))
        return length
    
