"""The recording handler mostly gets wavs from urls."""
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
import urllib2
import os
import shutil

class RecordingHandler(object):
    """Get files of recordings from urls"""
    def __init__(self):
        self.wav_base_url = "http://vocaroo.com/media_command.php?media=RECORDING_ID&command=download_wav"
        self.record_id_tag = "RECORDING_ID"
        self.recording_basedir = "/home/taylor/data/speech/resource_management_recordings/"
        
    def download_vocaroo_recording(self,url,type="wav"):
        if type=="wav":
            remote_file_id = os.path.basename(url)
            download_url = self.wav_base_url.replace(self.record_id_tag,remote_file_id)
            dest = os.path.join(self.recording_basedir,os.path.basename(url)+".wav")
            if not os.path.exists(dest):
                response = urllib2.urlopen(download_url).read()
                open(dest,"w").write(response)        
            return dest