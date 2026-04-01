from __future__ import annotations

import hashlib
import sys


def generate_table_name() -> str:
    caller_frame = sys._getframe(1)
    full_file_name = caller_frame.f_code.co_filename
    file_name = full_file_name.removesuffix(".py").split("/")[-1]
    func_name = caller_frame.f_code.co_qualname
    return f"{file_name}_{func_name}_{hashlib.md5(full_file_name.encode()).hexdigest()[:8]}"
