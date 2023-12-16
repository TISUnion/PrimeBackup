from typing import Optional

try:
	import pwd
except ImportError:
	pwd = None
try:
	import grp
except ImportError:
	grp = None


def uid_to_name(uid: int) -> Optional[str]:
	if pwd is not None:
		try:
			return pwd.getpwuid(uid).pw_name
		except KeyError:
			pass
	return None


def gid_to_name(gid: int) -> Optional[str]:
	if pwd is not None:
		try:
			return grp.getgrgid(gid).gr_name
		except KeyError:
			pass
	return None
