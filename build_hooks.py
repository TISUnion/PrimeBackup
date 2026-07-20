import shutil
from pathlib import Path

HERE = Path(__file__).parent
REPOS_ROOT = HERE.parent


def on_post_build(config, **kwargs):
	site_dir = Path(config['site_dir'])
	for file in ['requirements.txt', 'requirements.optional.txt']:
		shutil.copy(REPOS_ROOT / file, site_dir)
