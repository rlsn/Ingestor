"""
This script contains utils functions for the webui module
rlsn 2024
"""
from PIL.JpegImagePlugin import JpegImageFile
from nicegui import ui
import re
from ..utils import compute_subset_download
def is_subs_latest(subs_info):
    if compute_subset_download(subs_info)<=0:
        return ""
    all_latest = True
    for part_info in subs_info["partitions"].values():
        all_latest &= is_part_latest(part_info)!="No"
    return "Yes" if all_latest else "No"
    
def is_part_latest(part_info):
    if not part_info["downloaded"]:
        return ""
    return "Yes" if "is_latest" in part_info and part_info["is_latest"] else "No"

def stream_load_code_snippet(name, subs, parts=[]):
    code = f'import pygestor\n\nbatches = pygestor.stream_dataset("{name}", "{subs}"'
    if len(parts)>0:
        code += ', ["' + '","'.join(parts) +'"]'
    code += ', batch_size=16)\n\nfor bn, batch in enumerate(batches):\n    # do something'
    return code

def full_load_code_snippet(name, subs, parts=[]):
    code = f'import pygestor\n\nds = pygestor.load_dataset("{name}", "{subs}"'
    if len(parts)>0:
        code += ', ["' + '","'.join(parts) + '"]'
    code += ')'
    return code

def display_sample(sample):
    if type(sample)==str:
        if re.search("(jpg|png|gif|jpeg)$", sample, re.IGNORECASE):
            ui.image(sample).classes('w-[300px]')
        elif sample.endswith(".mp3"):
            a = ui.audio(sample)    
        else:
            ui.label(sample)
    elif type(sample)==JpegImageFile:
        ui.image(sample).classes('w-[300px]')
    else:
        ui.label(str(sample))