from typing import Optional

try:
	import pwd
except ImportError:
	pwd = None  # type: ignore[assignment]
try:
	import grp
except ImportError:
	grp = None  # type: ignore[assignment]


def uid_to_name(uid: int) -> Optional[str]:
	if hasattr(pwd, 'getpwuid') and callable(pwd.getpwuid):
		try:
			return pwd.getpwuid(uid).pw_name
		except KeyError:
			pass
	return None


def gid_to_name(gid: int) -> Optional[str]:
	if hasattr(grp, 'getgrgid') and callable(grp.getgrgid):
		try:
			return grp.getgrgid(gid).gr_name
		except KeyError:
			pass
	return None
