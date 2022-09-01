import unicodedata

def remove_combine_regex(string: str) -> str:
    normalized = unicodedata.normalize('NFD', string)
    return ''.join([l for l in normalized if not unicodedata.combining(l)])