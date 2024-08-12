def stream_load_code_snipet(name, subs, parts=[]):
    code = f'import ingestor\n\nbatches = ingestor.stream_dataset("{name}", "{subs}"'
    if len(parts)>0:
        code += ', ["' + '","'.join(parts) +'"]'
    code += ', batch_size=16)\n\nfor bn, batch in enumerate(batches):\n    # do something'
    return code

def full_load_code_snipet(name, subs, parts=[]):
    code = f'import ingestor\n\nds = ingestor.load_dataset("{name}", "{subs}"'
    if len(parts)>0:
        code += ', ["' + '","'.join(parts) + '"]'
    code += ')'
    return code