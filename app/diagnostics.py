from traceback import walk_tb


def safe_exception_trace(error: BaseException) -> str:
    # Exception messages and chained exceptions can contain URLs or SQL values.
    frames = [
        f"  {frame.f_code.co_filename}:{line} in {frame.f_code.co_name}"
        for frame, line in walk_tb(error.__traceback__)
    ]
    return "\n".join([type(error).__name__, *frames])
