"""Context-aware decoding; preserve both raw spelling and usable original URL."""
import html
import re
from collections.abc import Mapping
from urllib.parse import urlsplit
from .models import Referral

TOKEN = re.compile(r'https?://[^\s<>"\x27`]+', re.I)
PATH = re.compile(r"/referral/([A-Za-z0-9_-]+)\Z")
ESCAPES = re.compile(r'\\(?:u([0-9a-fA-F]{4})|([/_?&=\-]))')


def strings(value):
    if isinstance(value, str):
        yield value
    elif isinstance(value, Mapping):
        for child in value.values():
            yield from strings(child)
    elif isinstance(value, (list, tuple)):
        for child in value:
            yield from strings(child)


def decode(text):
    # Preserve a map from decoded offsets to the original text, including entities.
    chars, spans = [], []
    index = 0
    while index < len(text):
        match = ESCAPES.match(text, index)
        if match:
            val = chr(int(match[1], 16)) if match[1] else match[2]
            end = match.end()
        elif text[index] == "&" and (end := text.find(";", index, index + 15)) != -1:
            end += 1
            val = html.unescape(text[index:end])
        else:
            val, end = text[index], index + 1
        for char in val:
            chars.append(char)
            spans.append((index, end))
        index = end
    return "".join(chars), spans


def extract(content) -> list[Referral]:
    result, seen = [], set()
    for text in strings(content):
        decoded, spans = decode(text)
        for match in TOKEN.finditer(decoded):
            url = match[0].rstrip(".,;:!?)]}*")
            # A standalone URL with a query may legitimately end in punctuation.
            if match.start() == 0 and match.end() == len(decoded) and '?' in match[0]:
                url = match[0]
            try:
                parts = urlsplit(url)
                code = PATH.fullmatch(parts.path)
                if (parts.scheme.lower() not in {"http", "https"} or
                    parts.netloc.lower() != "claude.ai" or not code):
                    continue
            except ValueError:
                continue
            if url in seen:
                continue
            seen.add(url)
            raw = text[spans[match.start()][0]:spans[match.start() + len(url) - 1][1]]
            result.append(Referral(url, f"https://claude.ai/referral/{code[1]}", code[1], raw))
    return result
