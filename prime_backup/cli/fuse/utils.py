import errno
import functools
import inspect
from collections.abc import Callable
from typing import Optional

import fuse

from prime_backup.cli.fuse.config import FuseConfig
from prime_backup.logger import get as get_logger


class FuseErrnoReturnError(OSError):
	"""
	see fuse.ErrnoWrapper
	fuse can read the errno field
	"""
	def __init__(self, err: int):
		if not isinstance(err, int) or err <= 0:
			raise ValueError(err)
		self.errno = err


def fuse_operation_wrapper(func_name: Optional[str] = None) -> Callable:
	def decorator(func: Callable) -> Callable:
		logger = get_logger()

		@functools.wraps(func)
		def wrapper(*args):
			call_text = f'{func_name or func.__name__}({", ".join(map(repr, args[1:]))})'
			if FuseConfig.get().log_call:
				logger.info(f'CALL {call_text}')
			try:
				ret = func(*args)
			except FuseErrnoReturnError:
				raise
			except Exception as e:
				logger.exception('fuse_operation_wrapper catch {}'.format(type(e)))
				ret = -errno.EIO

			if FuseConfig.get().log_call:
				if ret is None or isinstance(ret, (int, fuse.Stat, fuse.Direntry)):
					ret_val = repr(ret)
				else:
					ret_val = str(type(ret))
				if isinstance(ret, (str, bytes, list, dict)):
					ret_val += f' (len={len(ret)})'
				logger.info(f'CALL {call_text} = {ret_val}')

			return ret

		wrapper.__signature__ = inspect.signature(func)
		return wrapper
	return decorator
