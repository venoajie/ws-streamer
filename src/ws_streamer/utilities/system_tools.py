# -*- coding: utf-8 -*-

import asyncio
import datetime
import os
import signal
import sys
from functools import lru_cache, wraps
from time import sleep

# https://python.plainenglish.io/five-python-wrappers-that-can-reduce-your-code-by-half-af775feb1d5


def get_ttl_hash(seconds=5):
    """Calculate hash value for TTL caching.

    Args:
        seconds (int, optional): Expiration time in seconds. Defaults to 3600.

    Returns:
        int: Hash value.
    """
    utime = datetime.datetime.now().timestamp()
    return round(utime / (seconds + 1))


def convert_size(size_bytes):
    """Convert bytes to human readable format."""
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size_bytes < 1024:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.2f} PB"


def get_file_size():
    """A decorator for TTL cache functionality.

    https://medium.com/@ryan_forrester_/getting-file-sizes-in-python-a-complete-guide-01293aaa68ef

    Args:

    """
    import os

    file_path = "error.log"
    size_bytes = convert_size(os.path.getsize(file_path))
    print(f"{file_path} file size: {size_bytes} bytes")


def ttl_cache(ttl_seconds=5):
    """A decorator for TTL cache functionality.

    https://kioku-space.com/en/python-ttl-cache-with-toggle/

    Args:
        ttl_seconds (int, optional): Expiration time in seconds. Defaults to 3600.
    """

    def ttl_cache_deco(func):
        """Returns a function with time-to-live (TTL) caching capabilities."""

        # Function with caching capability and dummy argument
        @lru_cache(maxsize=None)
        def cached_dummy_func(*args, ttl_dummy, **kwargs):
            del ttl_dummy  # Remove the dummy argument
            return func(*args, **kwargs)

        # Function to input the hash value into the dummy argument
        @wraps(func)
        def ttl_cached_func(*args, **kwargs):
            hash = get_ttl_hash(ttl_seconds)
            return cached_dummy_func(*args, ttl_dummy=hash, **kwargs)

        return ttl_cached_func

    return ttl_cache_deco


@ttl_cache(ttl_seconds=5)
def get_content():
    return "AAAAAAAAAAAAA"


print(get_content())


def get_platform() -> str:
    """
    Check current platform/operating system where app is running

    Args:
        None

    Returns:
        Current platform (str): linux, OS X, win

    References:
        https://www.webucator.com/article/how-to-check-the-operating-system-with-python/
        https://stackoverflow.com/questions/1325581/how-do-i-check-if-im-running-on-windows-in-python
    """

    platforms: dict = {
        "linux1": "linux",
        "linux2": "linux",
        "darwin": "OS X",
        "win32": "win",
    }

    if sys.platform not in platforms:
        return sys.platform

    return platforms[sys.platform]


def provide_path_for_file(
    end_point: str,
    marker: str = None,
    status: str = None,
    method: str = None,
) -> str:
    """

    Provide uniform format for file/folder path address

    Args:
        marker(str): currency, instrument, other
        end_point(str): orders, myTrades
        status(str): open, closed
        method(str): web/manual, api/bot

    Returns:
        Path address(str): in linux/windows format
    """
    from pathlib import Path

    current_os = get_platform()

    # Set root equal to  current folder
    root: str = Path(".")

    exchange = None

    if bool(
        [
            o
            for o in [
                "portfolio",
                "positions",
                "sub_accounts",
            ]
            if (o in end_point)
        ]
    ):
        exchange: str = "deribit"
        sub_folder: str = f"databases/exchanges/{exchange}/portfolio"

    if bool(
        [
            o
            for o in [
                "orders",
                "myTrades",
                "my_trades",
            ]
            if (o in end_point)
        ]
    ):
        exchange: str = "deribit"
        sub_folder: str = f"databases/exchanges/{exchange}/transactions"

    if bool(
        [
            o
            for o in [
                "ordBook",
                "index",
                "instruments",
                "currencies",
                "ohlc",
                "futures_analysis",
                "ticker-all",
                "ticker_all",
                "ticker",
            ]
            if (o in end_point)
        ]
    ):
        sub_folder = "databases/market"
        exchange = "deribit"

    if bool(
        [
            o
            for o in [
                "openInterestHistorical",
                "openInterestHistorical",
                "openInterestAggregated",
            ]
            if (o in end_point)
        ]
    ):
        sub_folder = "databases/market"
        exchange = "general"

    if marker != None:
        file_name = f"{marker.lower()}-{end_point}"

        if status != None:
            file_name = f"{file_name}-{status}"

        if method != None:
            file_name = f"{file_name}-{method}"

    else:
        file_name = f"{end_point}"

    if ".env" in end_point:
        sub_folder = "configuration"

    if "config_strategies.toml" in end_point:
        sub_folder = "strategies"

    if "api_url_end_point.toml" in end_point:
        sub_folder = "transaction_management/binance"

    # to accomodate pytest env
    if "test.env" in end_point:
        sub_folder = "src/configuration"
        end_point = ".env"

    config_file = ".env" in file_name or ".toml" in file_name

    file_name = (f"{end_point}") if config_file else (f"{file_name}.pkl")

    # Combine root + folders
    my_path_linux: str = (
        root / sub_folder if exchange == None else root / sub_folder / exchange
    )
    my_path_win: str = (
        root / "src" / sub_folder
        if exchange == None
        else root / "src" / sub_folder / exchange
    )

    if "portfolio" in sub_folder or "transactions" in sub_folder:
        my_path_linux: str = (
            root / sub_folder if exchange == None else root / sub_folder
        )
        my_path_win: str = (
            root / "src" / sub_folder if exchange == None else root / "src" / sub_folder
        )

    # Create target Directory if it doesn't exist in linux
    if not os.path.exists(my_path_linux) and current_os == "linux":
        os.makedirs(my_path_linux)

    return (
        (my_path_linux / file_name)
        if get_platform() == "linux"
        else (my_path_win / file_name)
    )


def is_current_file_running(script: str) -> bool:
    """
    Check current file is running/not. Could be used to avoid execute an already running file

    Args:
        script (str): name of the file

    Returns:
        Bool: True, file is running

    References:
        https://stackoverflow.com/questions/788411/check-to-see-if-python-script-is-running
    """
    import psutil

    for q in psutil.process_iter():

        if q.name().startswith("python") or q.name().startswith("py"):
            if (
                len(q.cmdline()) > 1
                and script in q.cmdline()[1]
                and q.pid != os.getpid()
            ):
                # print("'{}' Process is already running".format(script))
                return True

    return False


def reading_from_db_pickle(
    end_point,
    instrument: str = None,
    status: str = None,
) -> float:
    """ """
    from utilities import pickling

    return pickling.read_data(
        provide_path_for_file(
            end_point,
            instrument,
            status,
        )
    )


def sleep_and_restart_program(idle: float = None) -> None:
    """

    Halt the program for some seconds and restart it

    Args:
        idle (float): seconds of the program halted before restarted.
        None: restart is not needed

    Returns:
        None

    """

    if idle != None:
        print(f" sleep for {idle} seconds")
        sleep(idle)

    print(f"restart")
    python = sys.executable
    os.execl(
        python,
        python,
        *sys.argv,
    )


async def sleep_and_restart(idle: float = None) -> None:
    """

    Halt the program for some seconds and restart it

    Args:
        idle (float): seconds of the program halted before restarted.
        None: restart is not needed

    Returns:
        None

    """

    if idle != None:
        print(f" sleep for {idle} seconds")
        await asyncio.sleep(idle)

    print(f"restart")
    python = sys.executable
    os.execl(python, python, *sys.argv)


def exception_handler(func):
    # https://python.plainenglish.io/five-python-wrappers-that-can-reduce-your-code-by-half-af775feb1d5
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            # Handle the exception
            print(f"An exception occurred: {str(e)}")
            # Optionally, perform additional error handling or logging
            # Reraise the exception if needed

    return wrapper


def parse_error_message(
    error: str,
    message: str = None,
) -> str:
    """

    Capture & emit error message

    Args:
        message (str): error message

    Returns:
        trace back error message

    """

    import traceback

    from loguru import logger as log

    info = f"{error} \n \n {traceback.format_exc()}"

    if message != None:
        info = f"{message} \n \n {error} \n \n {traceback.format_exc()}"

    log.add(
        "error.log", backtrace=True, diagnose=True
    )  # Caution, may leak sensitive data in prod

    log.critical(f"{info}")

    return info


def raise_error_message(
    error: str,
    idle: float = None,
    message: str = None,
) -> None:
    """

    Capture & emit error message
    Optional: Send error message to telegram server

    Args:
        idle (float): seconds of the program halted before restarted. None: restart is not needed
        message (str): error message

    Returns:
        None

    Reference:
        https://medium.com/pipeline-a-data-engineering-resource/prettify-your-python-logs-with-loguru-a7630ef48d87

    """

    info = parse_error_message(error, message)

    if error == True:  # to respond 'def is_current_file_running'  result
        sys.exit(1)

    if idle == None:
        info = f"{error}"

    if idle != None:
        sleep_and_restart_program(idle)

    else:
        sys.exit()

    return info


async def async_raise_error_message(
    error: str,
    idle: float = None,
    message: str = None,
) -> None:
    """

    Capture & emit error message
    Optional: Send error message to telegram server

    Args:
        idle (float): seconds of the program halted before restarted. None: restart is not needed
        message (str): error message

    Returns:
        None

    Reference:
        https://medium.com/pipeline-a-data-engineering-resource/prettify-your-python-logs-with-loguru-a7630ef48d87

    """

    info = parse_error_message(error, message)

    if error == True:  # to respond 'def is_current_file_running'  result
        sys.exit(1)

    if idle != None:
        await sleep_and_restart(idle)

    else:
        sys.exit()

    return info


def check_file_attributes(filepath: str) -> None:
    """

    Check file attributes

    Args:
        filepath (str): name of the file

    Returns:
        st_mode=Inode protection mode
        st_ino=Inode number
        st_dev=Device inode resides on
        st_nlink=Number of links to the inode
        st_uid=User id of the owner
        st_gid=Group id of the owner
        st_size=Size in bytes of a plain file; amount of data waiting on some special files
        st_atime=Time of last access
        st_mtime=Time of last modification
        st_ctime=
            Unix: is the time of the last metadata change
            Windows: is the creation time (see platform documentation for details).

    Reference:
        https://medium.com/@BetterEverything/automate-removal-of-old-files-in-python-2085381fdf51
    """
    return os.stat(filepath)


def ipdb_sys_excepthook():
    """
    https://oscar-savolainen.medium.com/my-favourite-python-snippets-794d5653af38
    When called this function will set up the system exception hook.
        This hook throws one into an ipdb breakpoint if and where a system
        exception occurs in one's run.

        Example usage:
        >>> ipdb_sys_excepthook()
    """

    import sys
    import traceback

    import ipdb

    def info(type, value, tb):
        """
        System excepthook that includes an ipdb breakpoint.
        """
        if hasattr(sys, "ps1") or not sys.stderr.isatty():
            # we are in interactive mode or we don't have a tty-like
            # device, so we call the default hook
            sys.__excepthook__(type, value, tb)
        else:
            # we are NOT in interactive mode, print the exception...
            traceback.print_exception(type, value, tb)
            print
            # ...then start the debugger in post-mortem mode.
            # pdb.pm() # deprecated
            ipdb.post_mortem(tb)  # more "modern"

    sys.excepthook = info


def kill_process(process_name):
    """_summary_

    Args:
        process_ (str): _description_

    Returns:
        _type_: _description_

        https://www.geeksforgeeks.org/kill-a-process-by-name-using-python/
    """

    import signal

    try:

        # iterating through each instance of the process
        for line in os.popen("ps ax | grep " + process_name + " | grep -v grep"):
            fields = line.split()

            # extracting Process ID from the output
            pid = fields[0]

            # terminating process
            os.kill(int(pid), signal.SIGKILL)
        print("Process Successfully terminated")

    except:
        print("Error Encountered while running script")


async def back_up_db(idle_time: int):

    from db_management.sqlite_management import back_up_db_sqlite

    extensions = ".bak"

    while True:

        folder_path = "databases/"

        try:
            file_list = os.listdir(folder_path)

            for currentFile in file_list:
                # log.error(currentFile)
                if ".bak" in currentFile:
                    os.remove(f"{folder_path}{currentFile}")
            await back_up_db_sqlite()

        except Exception as error:

            parse_error_message(error)

        await asyncio.sleep(idle_time)


def main():
    print("Everything is going swimmingly")
    raise NotImplementedError("Oh no what happened?")


if __name__ == "__main__":
    ipdb_sys_excepthook()
    main()


class SignalHandler:
    """
    https://medium.com/@cziegler_99189/gracefully-shutting-down-async-multiprocesses-in-python-2223be384510
    """

    KEEP_PROCESSING = True

    def __init__(self):
        signal.signal(signal.SIGINT, self.exit_gracefully)

        signal.signal(signal.SIGTERM, self.exit_gracefully)

    def exit_gracefully(self, signum, frame):

        print(f"signum {signum} frame {frame}")
        print("Exiting gracefully")

        self.KEEP_PROCESSING = False


def handle_ctrl_c() -> None:
    """
    https://stackoverflow.com/questions/67866293/how-to-subscribe-to-multiple-websocket-streams-using-muiltiprocessing
    """
    signal.signal(signal.SIGINT, sys.exit(0))


def get_config_tomli(file_name: str) -> list:
    """ """
    import tomli

    config_path = provide_path_for_file(file_name)

    try:
        if os.path.exists(config_path):
            with open(config_path, "rb") as handle:
                read = tomli.load(handle)
                return read

    except Exception as error:
        parse_error_message(error)
        return []
