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
from copy import deepcopy as copy
def non_destructive(fn):
    def wrapped(*args, **kwargs):
        orig_args = copy(args)
        orig_kwargs = copy(kwargs)
        result = fn(*args,**kwargs)
        assert args == orig_args and kwargs == orig_kwargs
        return result        
    return wrapped

def destructive(fn):
    def wrapped(*args, **kwargs):
        orig_args = copy(args)
        orig_kwargs = copy(kwargs)
        result = fn(*args,**kwargs)
        assert args != orig_args and kwargs != orig_kwargs
        return result        
    return wrapped