from __future__ import annotations

import sys


def generate_table_name() -> str:
    caller_frame = sys._getframe(1)
    file_name = caller_frame.f_code.co_filename.removesuffix(".py").split("/")[-1]
    func_name = caller_frame.f_code.co_qualname
    return f"{file_name}_{func_name}"
