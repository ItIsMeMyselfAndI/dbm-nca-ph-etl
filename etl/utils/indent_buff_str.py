import io


def indent_buff_str(buff: io.StringIO, indention_count: int):
    string = buff.getvalue()
    indented_str = '\n'.join(
        f'{"\t" * indention_count}' + line for line in string.splitlines())
    return indented_str
