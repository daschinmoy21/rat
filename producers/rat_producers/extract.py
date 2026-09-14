import re 
_CASHTAG = re.compile(r"\$([A-Za-z]{1,5})\b")

def extract_entities(text:str)->list[str]:
    seen:set[str] = set()
    out:list[str] = []
    for m in _CASHTAG.finditer(text):
        tag = m.group(1).upper()
        if tag not in seen:
            seen.add(tag)
            out.append(tag)
    return out
