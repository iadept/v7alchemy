
def base36encode(number, alphabet='0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ'):
    base36 = ''
    sign = ''
    if 0 <= number < len(alphabet):
        return sign + alphabet[number]
    while number != 0:
        number, i = divmod(number, len(alphabet))
        base36 = alphabet[i] + base36
    return sign + base36


def base36decode(number):
    return int(number, 36)


def doc_index(doc: str) -> int:
    return base36decode(doc[0:4])


def doc_id(doc: str) -> str:
    return doc[4:]